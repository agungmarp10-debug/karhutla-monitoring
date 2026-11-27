import os
import requests
import pandas as pd
import psycopg

from io import StringIO
from dotenv import load_dotenv

load_dotenv()

MPOB_URL = (
    "https://bepi.mpob.gov.my/admin2/"
    "price_local_daily_view_cpo_msia.php"
)


# 1. Extract and reshape CPO price data
def extract_cpo(year):

    params = {
        "jenis": "1Y",
        "more": "Y",
        "tahun": str(year)
    }

    response = requests.get(
        MPOB_URL,
        params=params,
        timeout=60
    )

    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))

    cpo_raw = tables[2]

    # Extract daily records and month headers
    cpo = cpo_raw.iloc[3:34].copy()
    cpo.columns = cpo_raw.iloc[2]

    cpo = cpo.rename(columns={"Date": "day"})

    # Convert wide table into long format
    cpo = cpo.melt(
        id_vars="day",
        var_name="month",
        value_name="price_raw"
    )

    # Construct calendar dates
    cpo["price_date"] = pd.to_datetime(
        cpo["day"].astype(str).str.strip()
        + " "
        + cpo["month"].astype(str).str.strip()
        + " "
        + str(year),
        format="%d %B %Y",
        errors="coerce"
    )

    # Remove invalid calendar dates
    cpo = cpo.dropna(subset=["price_date"])

    # Remove future dates
    today = pd.Timestamp.now(
        tz="Asia/Kuala_Lumpur"
    ).date()

    cpo["price_date"] = cpo["price_date"].dt.date

    cpo = cpo[cpo["price_date"] <= today]

    cpo = cpo[["price_date", "price_raw"]].copy()

    cpo["price_raw"] = cpo["price_raw"].astype("string")

    if cpo.empty:
        raise ValueError(
            f"No valid CPO records found for {year}"
        )

    print(f"Year {year}: Extracted {len(cpo)} rows")

    return cpo


# 2. Load CPO data into PostgreSQL
def load_cpo(df):

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    query = """
        INSERT INTO bronze.raw_cpo (
            price_date,
            price_raw
        )
        VALUES (%s, %s)
        ON CONFLICT (price_date)
        DO UPDATE SET
            price_raw = EXCLUDED.price_raw,
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

            print("Upserted CPO rows:", cursor.rowcount)


# 3. Historical and scheduled ingestion
def ingest_cpo(start_year, end_year):

    for year in range(start_year, end_year + 1):

        df = extract_cpo(year)

        load_cpo(df)

    print("MPOB CPO ingestion completed.")


if __name__ == "__main__":

    ingest_cpo(
        start_year=2025,
        end_year=2026
    )