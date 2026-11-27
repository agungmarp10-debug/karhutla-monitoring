import os
import requests
import pandas as pd
import time
import psycopg

from io import StringIO
from datetime import date, timedelta
from extract_firms import extract_firms, load_firms
from dotenv import load_dotenv

load_dotenv()


AREA = "94,-7,120,8"

def get_covered_dates(start_date, end_date, source, area):

    query = """
        SELECT coverage_date
        FROM bronze.fire_ingestion_coverage
        WHERE coverage_date BETWEEN %s AND %s
          AND source = %s
          AND bounding_box = %s;
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
                (start_date, end_date, source, area)
            )

            return {
                row[0] for row in cursor.fetchall()
            }


def backfill_firms(start_date, end_date, source, area):

    current_date = date.fromisoformat(start_date)
    end_date = date.fromisoformat(end_date)

    covered_dates = get_covered_dates(
        current_date,
        end_date,
        source,
        area
    )

    while current_date <= end_date:

        if current_date in covered_dates:

            current_date += timedelta(days=1)
            continue

        day_range = 1

        while day_range < 5:

            next_date = current_date + timedelta(
                days=day_range
            )

            if (
                next_date > end_date
                or next_date in covered_dates
            ):
                break

            day_range += 1

        print(
            f"Processing {source}: "
            f"{current_date} | {day_range} days"
        )

        df = extract_firms(
            source=source,
            area=area,
            day_range=day_range,
            start_date=current_date.isoformat()
        )

        load_firms(
            df=df,
            source=source,
            area=area,
            start_date=current_date.isoformat(),
            day_range=day_range
        )

        current_date += timedelta(days=day_range)

    print(f"{source} backfill completed.")


def get_firms_availability():

    map_key = os.getenv("NASA_FIRMS_MAP_KEY")

    if not map_key:
        raise ValueError("NASA FIRMS MAP_KEY not found.")

    url = (
        "https://firms.modaps.eosdis.nasa.gov/"
        f"api/data_availability/csv/{map_key}/all"
    )

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
                    "NASA FIRMS availability connection failed "
                    "after 3 attempts."
                ) from None

            print("Retrying in 30 seconds...")
            time.sleep(30)

    if response.status_code != 200:
        raise RuntimeError(
            f"NASA FIRMS API failed: HTTP {response.status_code}"
        )

    return pd.read_csv(StringIO(response.text))

def ingest_historical_firms(start_date, end_date):

    availability = get_firms_availability()

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    for source in [
        "VIIRS_NOAA20_SP",
        "VIIRS_NOAA20_NRT"
    ]:

        selected = availability[
            availability["data_id"] == source
        ].iloc[0]

        available_start = date.fromisoformat(
            selected["min_date"]
        )

        available_end = date.fromisoformat(
            selected["max_date"]
        )

        actual_start = max(start, available_start)
        actual_end = min(end, available_end)

        if actual_start > actual_end:
            continue

        if actual_start > start:
            raise ValueError(
                f"Missing FIRMS coverage starting {start}"
            )

        backfill_firms(
            start_date=actual_start.isoformat(),
            end_date=actual_end.isoformat(),
            source=source,
            area=AREA
        )

        start = actual_end + timedelta(days=1)

        if start > end:
            break

    if start <= end:
        raise ValueError(
            f"Missing FIRMS coverage starting {start}"
        )

    print("NASA FIRMS historical ingestion completed.")


if __name__ == "__main__":

    ingest_historical_firms(
        start_date="2025-01-01",
        end_date="2026-11-25"
    )