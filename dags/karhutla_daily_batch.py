from datetime import timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract_firms import extract_firms, load_firms
from backfill_firms import ingest_historical_firms
from extract_weather import ingest_weather
from extract_cpo import ingest_cpo

from transform_silver import (
    get_connection,
    populate_dimensions,
    transform_fire,
    transform_weather,
    transform_cpo
)


BASE_DIR = Path(__file__).resolve().parents[1]

HISTORICAL_START = "2025-01-01"
AREA = "94,-7,120,8"


def initialize_warehouse():

    sql_files = [
        "01_create_schemas.sql",
        "02_create_bronze_tables.sql",
        "03_create_silver_tables.sql",
        "04_create_gold_tables.sql",
        "06_create_fire_coverage.sql"
    ]

    with get_connection() as conn:

        with conn.cursor() as cursor:

            for filename in sql_files:

                sql_file = BASE_DIR / "sql" / filename

                query = sql_file.read_text(
                    encoding="utf-8"
                )

                cursor.execute(query)

                print(f"Executed: {filename}")

    print("Warehouse initialization completed.")


def run_firms():

    today = pendulum.now("UTC").date()

    end_date = today - timedelta(days=2)

    ingest_historical_firms(
        start_date=HISTORICAL_START,
        end_date=end_date.isoformat()
    )

    refresh_start = end_date - timedelta(days=4)

    df = extract_firms(
        source="VIIRS_NOAA20_NRT",
        area=AREA,
        day_range=5,
        start_date=refresh_start.isoformat()
    )

    load_firms(
        df=df,
        source="VIIRS_NOAA20_NRT",
        area=AREA,
        start_date=refresh_start.isoformat(),
        day_range=5
    )

    print("NASA FIRMS ingestion completed.")


def run_weather():

    today = pendulum.now("Asia/Jakarta").date()

    end_date = today - timedelta(days=5)

    ingest_weather(
        start_date=HISTORICAL_START,
        end_date=end_date.isoformat(),
        skip_existing=True
    )


def run_cpo():

    current_year = pendulum.now(
        "Asia/Jakarta"
    ).year

    ingest_cpo(
        start_year=2025,
        end_year=current_year
    )


def run_silver():

    populate_dimensions()

    transform_fire()

    transform_weather()

    transform_cpo()


def run_gold():

    sql_file = (
        BASE_DIR / "sql" / "05_transform_gold.sql"
    )

    query = sql_file.read_text(
        encoding="utf-8"
    )

    with get_connection() as conn:

        conn.autocommit = True

        with conn.cursor() as cursor:

            cursor.execute(query)

    print("Gold transformation completed.")


default_args = {
    "owner": "karhutla",
    "retries": 2,
    "retry_delay": timedelta(minutes=15)
}


with DAG(

    dag_id="karhutla_daily_batch",

    description="Daily Karhutla monitoring ELT pipeline",

    default_args=default_args,

    start_date=pendulum.datetime(
        2026, 11, 26,
        tz="Asia/Jakarta"
    ),

    schedule="0 7 * * *",

    catchup=False,

    max_active_runs=1,

    tags=["karhutla", "daily", "elt"]

) as dag:


    init_task = PythonOperator(
        task_id="initialize_warehouse",
        python_callable=initialize_warehouse
    )


    firms_task = PythonOperator(
        task_id="ingest_firms",
        python_callable=run_firms
    )


    weather_task = PythonOperator(
        task_id="ingest_weather",
        python_callable=run_weather,
        retries=12,
        retry_delay=timedelta(hours=2)
    )


    cpo_task = PythonOperator(
        task_id="ingest_cpo",
        python_callable=run_cpo
    )


    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver
    )


    gold_task = PythonOperator(
        task_id="transform_gold",
        python_callable=run_gold
    )


    init_task >> [
        firms_task,
        weather_task,
        cpo_task
    ] >> silver_task >> gold_task