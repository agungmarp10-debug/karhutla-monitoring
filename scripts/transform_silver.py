import os
import pandas as pd
import psycopg
import geopandas as gpd

from download_boundaries import download_boundaries
from dotenv import load_dotenv
from extract_weather import generate_weather_locations

load_dotenv()


def get_connection():

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


def populate_dimensions():

    locations = generate_weather_locations()

    provinces = locations[
        ["province", "timezone"]
    ].drop_duplicates().copy()

    provinces["island"] = provinces["province"].apply(
        lambda x: "Kalimantan"
        if "Kalimantan" in x
        else "Sumatra"
    )

    with get_connection() as conn:

        with conn.cursor() as cursor:

            for row in provinces.itertuples(index=False):

                cursor.execute(
                    """
                    INSERT INTO silver.dim_location (
                        province_name,
                        island,
                        timezone
                    )
                    VALUES (%s, %s, %s)

                    ON CONFLICT (province_name)
                    DO UPDATE SET
                        island = EXCLUDED.island,
                        timezone = EXCLUDED.timezone;
                    """,
                    (
                        row.province,
                        row.island,
                        row.timezone
                    )
                )

            cursor.execute(
                """
                SELECT province_name, province_id
                FROM silver.dim_location;
                """
            )

            province_mapping = dict(cursor.fetchall())

            for row in locations.itertuples(index=False):

                cursor.execute(
                    """
                    INSERT INTO silver.dim_weather_location (
                        province_id,
                        point_number,
                        latitude,
                        longitude
                    )
                    VALUES (%s, %s, %s, %s)

                    ON CONFLICT (province_id, point_number)
                    DO UPDATE SET
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude;
                    """,
                    (
                        province_mapping[row.province],
                        row.point_number,
                        row.latitude,
                        row.longitude
                    )
                )

    print("Geographic dimensions populated.")
    print("Provinces:", len(provinces))
    print("Weather sampling locations:", len(locations))

def transform_fire():

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT DISTINCT ON (
                    latitude,
                    longitude,
                    acq_date,
                    acq_time,
                    satellite,
                    instrument
                )
                    latitude,
                    longitude,
                    acq_date,
                    acq_time,
                    confidence,
                    frp
                FROM bronze.raw_fire
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND acq_date IS NOT NULL
                  AND acq_time ~ '^[0-9]{4}$'
                ORDER BY
                    latitude,
                    longitude,
                    acq_date,
                    acq_time,
                    satellite,
                    instrument,
                    CASE
                        WHEN version LIKE '%NRT%' THEN 1
                        ELSE 0
                    END,
                    ingested_at DESC,
                    id DESC;
                """
            )

            columns = [
                col.name for col in cursor.description
            ]

            fire = pd.DataFrame(
                cursor.fetchall(),
                columns=columns
            )

            cursor.execute(
                """
                SELECT province_id, province_name, timezone
                FROM silver.dim_location;
                """
            )

            provinces = pd.DataFrame(
                cursor.fetchall(),
                columns=[
                    "province_id",
                    "province_name",
                    "timezone"
                ]
            )

    print("Deduplicated fire records:", len(fire))

    if fire.empty:
        raise ValueError("No fire observations found.")

    boundary_file = download_boundaries()

    boundaries = gpd.read_file(
        boundary_file
    ).to_crs("EPSG:4326")

    boundaries = boundaries.merge(
        provinces,
        left_on="shapeName",
        right_on="province_name",
        how="inner"
    )

    fire_geo = gpd.GeoDataFrame(
        fire,
        geometry=gpd.points_from_xy(
            fire["longitude"],
            fire["latitude"]
        ),
        crs="EPSG:4326"
    )

    mapped = gpd.sjoin(
        fire_geo,
        boundaries[
            ["province_id", "timezone", "geometry"]
        ],
        how="inner",
        predicate="within"
    )

    mapped = mapped.drop_duplicates(
        subset=None
    )

    print("Hotspots within target provinces:", len(mapped))

    mapped["acq_datetime"] = pd.to_datetime(
        mapped["acq_date"].astype(str)
        + " "
        + mapped["acq_time"].str.zfill(4),
        format="%Y-%m-%d %H%M",
        utc=True
    )

    for timezone in mapped["timezone"].unique():

        mask = mapped["timezone"] == timezone

        mapped.loc[mask, "fire_date"] = (
            mapped.loc[mask, "acq_datetime"]
            .dt.tz_convert(timezone)
            .dt.date
        )

    mapped["low_confidence"] = (
        mapped["confidence"] == "l"
    ).astype(int)

    mapped["nominal_confidence"] = (
        mapped["confidence"] == "n"
    ).astype(int)

    mapped["high_confidence"] = (
        mapped["confidence"] == "h"
    ).astype(int)

    daily = mapped.groupby(
        ["province_id", "fire_date"],
        as_index=False
    ).agg(
        hotspot_count=("latitude", "size"),
        low_confidence_count=("low_confidence", "sum"),
        nominal_confidence_count=("nominal_confidence", "sum"),
        high_confidence_count=("high_confidence", "sum"),
        avg_frp=("frp", "mean")
    )

    query = """
        INSERT INTO silver.fact_daily_fire (
            province_id,
            fire_date,
            hotspot_count,
            low_confidence_count,
            nominal_confidence_count,
            high_confidence_count,
            avg_frp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (province_id, fire_date)
        DO UPDATE SET
            hotspot_count = EXCLUDED.hotspot_count,
            low_confidence_count = EXCLUDED.low_confidence_count,
            nominal_confidence_count = EXCLUDED.nominal_confidence_count,
            high_confidence_count = EXCLUDED.high_confidence_count,
            avg_frp = EXCLUDED.avg_frp,
            updated_at = CURRENT_TIMESTAMP;
    """

    records = list(
        daily.astype(object)
        .where(pd.notna(daily), None)
        .itertuples(index=False, name=None)
    )

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                "DELETE FROM silver.fact_daily_fire;"
            )

            cursor.executemany(query, records)

    print("Daily fire records loaded:", len(daily))
    print("Fire transformation completed.")

def transform_weather():

    query = """
        INSERT INTO silver.fact_daily_weather (
            province_id,
            weather_date,
            avg_temperature,
            avg_precipitation,
            avg_humidity,
            sampling_points_count,
            valid_temperature_points,
            valid_precipitation_points,
            valid_humidity_points
        )

        SELECT
            l.province_id,
            w.weather_date,
            AVG(w.temperature_2m_mean),
            AVG(w.precipitation_sum),
            AVG(w.relative_humidity_2m_mean),
            COUNT(DISTINCT l.weather_location_id),
            COUNT(w.temperature_2m_mean),
            COUNT(w.precipitation_sum),
            COUNT(w.relative_humidity_2m_mean)

        FROM bronze.raw_weather w

        INNER JOIN silver.dim_weather_location l
            ON w.latitude = l.latitude
            AND w.longitude = l.longitude

        GROUP BY
            l.province_id,
            w.weather_date

        ON CONFLICT (province_id, weather_date)
        DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            avg_precipitation = EXCLUDED.avg_precipitation,
            avg_humidity = EXCLUDED.avg_humidity,
            sampling_points_count = EXCLUDED.sampling_points_count,
            valid_temperature_points = EXCLUDED.valid_temperature_points,
            valid_precipitation_points = EXCLUDED.valid_precipitation_points,
            valid_humidity_points = EXCLUDED.valid_humidity_points,
            updated_at = CURRENT_TIMESTAMP;
    """

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(query)

            print(
                "Daily weather records loaded:",
                cursor.rowcount
            )

    print("Weather transformation completed.")

def transform_cpo():

    query = """
        INSERT INTO silver.fact_daily_cpo (
            price_date,
            cpo_price_myr
        )

        SELECT
            price_date,
            REPLACE(TRIM(price_raw), ',', '')::NUMERIC(12, 2)

        FROM bronze.raw_cpo

        WHERE TRIM(price_raw) ~
            '^[0-9]+(,[0-9]{3})*([.][0-9]+)?$'

        ON CONFLICT (price_date)
        DO UPDATE SET
            cpo_price_myr = EXCLUDED.cpo_price_myr,
            updated_at = CURRENT_TIMESTAMP;
    """

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                "DELETE FROM silver.fact_daily_cpo;"
            )

            cursor.execute(query)

            print(
                "Daily CPO records loaded:",
                cursor.rowcount
            )

    print("CPO transformation completed.")

if __name__ == "__main__":

    populate_dimensions()

    transform_fire()

    transform_weather()

    transform_cpo()