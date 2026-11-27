import os
import requests
import pandas as pd
import psycopg
import time

from dotenv import load_dotenv
from io import StringIO
from datetime import date, timedelta

load_dotenv()


def extract_firms(
    source="VIIRS_NOAA20_NRT",
    area="94,-7,120,8",
    day_range=5,
    start_date=None
):
    map_key = os.getenv("NASA_FIRMS_MAP_KEY")

    if not map_key:
        raise ValueError("NASA FIRMS MAP_KEY not found.")

    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{map_key}/{source}/{area}/{day_range}"
    )

    if start_date:
        url += f"/{start_date}"

    max_retries = 3

    for attempt in range(max_retries):

        try:
            response = requests.get(url, timeout=120)
            break

        except requests.exceptions.RequestException as error:

            print(
                f"Connection failed: {type(error).__name__}. "
                f"Attempt {attempt + 1}/{max_retries}"
            )

            if attempt == max_retries - 1:
                raise RuntimeError(
                    "NASA FIRMS connection failed after 3 attempts."
                ) from None

            print("Retrying in 30 seconds...")
            time.sleep(30)

    if response.status_code != 200:
        raise RuntimeError(
            f"NASA FIRMS API failed: HTTP {response.status_code}"
        )

    df = pd.read_csv(
        StringIO(response.text),
        dtype={"acq_time": str}
    )

    required_columns = [
        "latitude", "longitude", "bright_ti4",
        "scan", "track", "acq_date", "acq_time",
        "satellite", "instrument", "confidence",
        "version", "bright_ti5", "frp", "daynight"
    ]

    if not set(required_columns).issubset(df.columns):
        raise ValueError("Unexpected NASA FIRMS data structure.")

    df = df[required_columns].copy()
    df["acq_time"] = df["acq_time"].str.zfill(4)

    print("Extracted rows:", len(df))

    return df


def load_firms(
    df,
    source=None,
    area=None,
    start_date=None,
    day_range=None
):

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    query = """
        INSERT INTO bronze.raw_fire (
            latitude, longitude, bright_ti4,
            scan, track, acq_date, acq_time,
            satellite, instrument, confidence,
            version, bright_ti5, frp, daynight
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING;
    """

    records = list(
        df.astype(object)
        .where(pd.notna(df), None)
        .itertuples(index=False, name=None)
    )

    with conn:
        with conn.cursor() as cursor:

            cursor.executemany(query, records)

            print("Inserted rows:", cursor.rowcount)

            if all(
                value is not None
                for value in [
                    source,
                    area,
                    start_date,
                    day_range
                ]
            ):

                coverage_query = """
                    INSERT INTO bronze.fire_ingestion_coverage (
                        coverage_date,
                        source,
                        bounding_box,
                        records_extracted
                    )
                    VALUES (%s, %s, %s, %s)

                    ON CONFLICT (
                        coverage_date,
                        source,
                        bounding_box
                    )
                    DO UPDATE SET
                        records_extracted =
                            EXCLUDED.records_extracted,
                        ingested_at = CURRENT_TIMESTAMP;
                """

                daily_counts = df.groupby(
                    "acq_date"
                ).size().to_dict()

                first_date = date.fromisoformat(start_date)

                coverage_records = []

                for i in range(day_range):

                    current_date = first_date + timedelta(days=i)

                    records_extracted = int(
                        daily_counts.get(
                            current_date.isoformat(), 0
                        )
                    )

                    coverage_records.append((
                        current_date,
                        source,
                        area,
                        records_extracted
                    ))

                cursor.executemany(
                    coverage_query,
                    coverage_records
                )

                print(
                    "Coverage days registered:",
                    len(coverage_records)
                )


if __name__ == "__main__":
    df = extract_firms()
    load_firms(df)

    print("NASA FIRMS ingestion completed.")