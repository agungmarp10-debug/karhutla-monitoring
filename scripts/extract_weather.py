import os
import requests
import pandas as pd
import psycopg
import geopandas as gpd

from pathlib import Path
from shapely.geometry import Point
from dotenv import load_dotenv
from download_boundaries import download_boundaries

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]

LOCATIONS_FILE = (
    BASE_DIR / "data" / "geospatial" / "weather_locations.csv"
)

PROVINCES = {
    "Aceh": "Asia/Jakarta",
    "North Sumatra": "Asia/Jakarta",
    "West Sumatra": "Asia/Jakarta",
    "Riau": "Asia/Jakarta",
    "Riau Islands": "Asia/Jakarta",
    "Jambi": "Asia/Jakarta",
    "South Sumatra": "Asia/Jakarta",
    "Bengkulu": "Asia/Jakarta",
    "Lampung": "Asia/Jakarta",
    "Bangka-Belitung Islands": "Asia/Jakarta",
    "West Kalimantan": "Asia/Jakarta",
    "Central Kalimantan": "Asia/Jakarta",
    "South Kalimantan": "Asia/Makassar",
    "East Kalimantan": "Asia/Makassar",
    "North Kalimantan": "Asia/Makassar"
}


# 1. Generate weather sampling locations
def generate_weather_locations(points_per_province=10):

    if LOCATIONS_FILE.exists():
        print("Using existing weather sampling locations.")
        return pd.read_csv(LOCATIONS_FILE)

    boundary_file = download_boundaries()

    provinces = gpd.read_file(boundary_file)
    provinces = provinces.to_crs("EPSG:4326")

    records = []

    for province, timezone in PROVINCES.items():

        selected = provinces[
            provinces["shapeName"] == province
        ]

        if selected.empty:
            raise ValueError(f"Province not found: {province}")

        geometry = selected.geometry.union_all()

        minx, miny, maxx, maxy = geometry.bounds

        candidates = []
        grid_size = 40

        for i in range(grid_size):
            for j in range(grid_size):

                lon = minx + (i + 0.5) * (
                    maxx - minx
                ) / grid_size

                lat = miny + (j + 0.5) * (
                    maxy - miny
                ) / grid_size

                point = Point(lon, lat)

                if geometry.contains(point):
                    candidates.append(point)

        if not candidates:
            candidates = [geometry.representative_point()]

        center = geometry.representative_point()

        selected_points = [
            min(candidates, key=lambda p: p.distance(center))
        ]

        while len(selected_points) < min(
            points_per_province, len(candidates)
        ):

            next_point = max(
                candidates,
                key=lambda p: min(
                    p.distance(s) for s in selected_points
                )
            )

            selected_points.append(next_point)

        for index, point in enumerate(
            selected_points, start=1
        ):

            records.append({
                "province": province,
                "point_number": index,
                "latitude": round(point.y, 6),
                "longitude": round(point.x, 6),
                "timezone": timezone
            })

        print(province, ":", len(selected_points), "points")

    df = pd.DataFrame(records)

    LOCATIONS_FILE.parent.mkdir(
        parents=True, exist_ok=True
    )

    df.to_csv(LOCATIONS_FILE, index=False)

    print("Total sampling locations:", len(df))

    return df


# 2. Extract weather data
def extract_weather(locations, start_date, end_date, timezone):

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": ",".join(
            locations["latitude"].astype(str)
        ),
        "longitude": ",".join(
            locations["longitude"].astype(str)
        ),
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_mean",
            "precipitation_sum",
            "relative_humidity_2m_mean"
        ],
        "timezone": timezone
    }

    response = requests.get(
        url,
        params=params,
        timeout=120
    )

    response.raise_for_status()

    data = response.json()

    if isinstance(data, dict):
        data = [data]

    if len(data) != len(locations):
        raise ValueError(
            "Unexpected number of weather locations returned."
        )

    expected_dates = pd.date_range(
        start_date,
        end_date
    ).strftime("%Y-%m-%d").tolist()

    frames = []

    for location, result in zip(
        locations.itertuples(index=False),
        data
    ):

        df = pd.DataFrame(result["daily"])

        df = df.rename(
            columns={"time": "weather_date"}
        )

        if df["weather_date"].tolist() != expected_dates:
            raise ValueError(
                "Incomplete weather date coverage."
            )

        df["latitude"] = location.latitude
        df["longitude"] = location.longitude

        df = df[[
            "latitude",
            "longitude",
            "weather_date",
            "temperature_2m_mean",
            "precipitation_sum",
            "relative_humidity_2m_mean"
        ]]

        frames.append(df)

    combined = pd.concat(
        frames,
        ignore_index=True
    )

    print("Locations extracted:", len(locations))
    print("Weather rows extracted:", len(combined))

    return combined


# 3. Load weather data into PostgreSQL
def load_weather(df):

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    query = """
        INSERT INTO bronze.raw_weather (
            latitude,
            longitude,
            weather_date,
            temperature_2m_mean,
            precipitation_sum,
            relative_humidity_2m_mean
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, weather_date)
        DO UPDATE SET
            temperature_2m_mean =
                EXCLUDED.temperature_2m_mean,
            precipitation_sum =
                EXCLUDED.precipitation_sum,
            relative_humidity_2m_mean =
                EXCLUDED.relative_humidity_2m_mean,
            ingested_at = CURRENT_TIMESTAMP;
    """

    records = list(
        df.astype(object)
        .where(pd.notna(df), None)
        .itertuples(index=False, name=None)
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, records)

            print("Upserted weather rows:", cursor.rowcount)

def get_missing_date_ranges(
    latitude,
    longitude,
    start_date,
    end_date
):

    query = """
        SELECT weather_date
        FROM bronze.raw_weather
        WHERE latitude = %s
          AND longitude = %s
          AND weather_date BETWEEN %s AND %s
          AND temperature_2m_mean IS NOT NULL
          AND precipitation_sum IS NOT NULL
          AND relative_humidity_2m_mean IS NOT NULL;
    """

    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    ) as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                query,
                (
                    latitude,
                    longitude,
                    start_date,
                    end_date
                )
            )

            existing_dates = {
                row[0] for row in cursor.fetchall()
            }

    expected_dates = pd.date_range(
        start_date,
        end_date
    ).date

    missing_dates = [
        day for day in expected_dates
        if day not in existing_dates
    ]

    if not missing_dates:
        return []

    ranges = []

    range_start = missing_dates[0]
    previous_date = missing_dates[0]

    for current_date in missing_dates[1:]:

        if (current_date - previous_date).days > 1:

            ranges.append((
                range_start.isoformat(),
                previous_date.isoformat()
            ))

            range_start = current_date

        previous_date = current_date

    ranges.append((
        range_start.isoformat(),
        previous_date.isoformat()
    ))

    return ranges

# 4. Execute weather ingestion for all locations
def ingest_weather(
    start_date,
    end_date,
    skip_existing=True
):

    locations = generate_weather_locations()

    pending = []

    for row in locations.itertuples(index=False):

        if skip_existing:

            missing_ranges = get_missing_date_ranges(
                latitude=row.latitude,
                longitude=row.longitude,
                start_date=start_date,
                end_date=end_date
            )

        else:

            missing_ranges = [
                (start_date, end_date)
            ]

        for range_start, range_end in missing_ranges:

            pending.append({
                "province": row.province,
                "point_number": row.point_number,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "timezone": row.timezone,
                "start_date": range_start,
                "end_date": range_end
            })

    if not pending:

        print("All requested weather data already loaded.")
        return

    pending_df = pd.DataFrame(pending)

    print("Locations requiring ingestion:",
          pending_df[["latitude", "longitude"]]
          .drop_duplicates().shape[0])

    groups = pending_df.groupby([
        "province",
        "timezone",
        "start_date",
        "end_date"
    ])

    for group_key, group in groups:

        province, timezone, range_start, range_end = group_key

        for i in range(0, len(group), 10):

            batch = group.iloc[i:i + 10]

            print(
                f"Processing {province}: "
                f"{len(batch)} locations | "
                f"{range_start} to {range_end}"
            )

            try:

                df = extract_weather(
                    locations=batch,
                    start_date=range_start,
                    end_date=range_end,
                    timezone=timezone
                )

                load_weather(df)

            except requests.exceptions.HTTPError as error:

                if error.response.status_code == 429:

                    print("Open-Meteo API rate limit reached.")
                    print("Existing progress has been saved.")
                    print("Retry will resume missing data only.")

                raise

    print("Open-Meteo ingestion completed.")

if __name__ == "__main__":

    ingest_weather(
        start_date="2025-01-01",
        end_date="2026-10-31",
        skip_existing=True
    )