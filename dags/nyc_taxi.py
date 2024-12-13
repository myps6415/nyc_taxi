import logging
from datetime import datetime, timedelta
from functools import wraps
from io import StringIO

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def error_handling_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {str(e)}")
            raise AirflowException(f"Data download failed: {str(e)}")
        except pd.errors.EmptyDataError as e:
            logging.error(f"Empty data received: {str(e)}")
            raise AirflowException(f"Empty data error: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            raise AirflowException(f"Task failed: {str(e)}")

    return wrapper


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 12, 31),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@error_handling_wrapper
def load_taxi_data(**context):
    """載入指定期間的計程車數據"""
    execution_date = context["logical_date"].replace(tzinfo=None)

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    check_sql = """
    SELECT COUNT(*) 
    FROM taxi_trips_staging 
    WHERE DATE_TRUNC('month', pickup_datetime AT TIME ZONE 'UTC') = 
          DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
    """

    count = pg_hook.get_first(
        check_sql, parameters=(execution_date.strftime("%Y-%m-%d"),)
    )[0]

    if count > 0:
        logging.info(
            f"Data for {execution_date.strftime('%Y-%m')} already exists, skipping..."
        )
        return

    base_url = "https://data.cityofnewyork.us/resource/qp3b-zxtp.json"
    where_clause = f"extract_month(pickup_datetime) = {execution_date.month} AND extract_year(pickup_datetime) = {execution_date.year}"

    offset = 0
    batch_size = 50000
    total_records = 0

    while True:
        params = {
            "$limit": batch_size,
            "$offset": offset,
            "$where": where_clause,  # 使用 $where 而不是 $query
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()

        records = response.json()
        if not records:
            break

        df = pd.DataFrame(records)

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
        df["passenger_count"] = pd.to_numeric(df["passenger_count"])
        df["trip_distance"] = pd.to_numeric(df["trip_distance"])
        df["fare_amount"] = pd.to_numeric(df["fare_amount"])
        df["tip_amount"] = pd.to_numeric(df["tip_amount"])
        df["total_amount"] = pd.to_numeric(df["total_amount"])

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cur.copy_expert(
                    """
                    COPY taxi_trips_staging (
                        pickup_datetime, dropoff_datetime, passenger_count,
                        trip_distance, pickup_latitude, pickup_longitude,
                        dropoff_latitude, dropoff_longitude, fare_amount,
                        tip_amount, total_amount, payment_type, rate_code
                    ) FROM STDIN WITH CSV
                    """,
                    buffer,
                )

        total_records += len(df)
        offset += batch_size
        logging.info(f"Loaded {len(df)} records for {execution_date.strftime('%Y-%m')}")

    log_sql = """
    INSERT INTO taxi_data_processing_log (
        processing_date, year_month, records_processed, status
    ) VALUES (%s, %s, %s, 'completed')
    """
    pg_hook.run(
        log_sql,
        parameters=(datetime.now(), execution_date.strftime("%Y-%m"), total_records),
    )


@error_handling_wrapper
def validate_data(**context):
    """執行數據質量檢查"""
    execution_date = context["logical_date"].replace(tzinfo=None)
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    validation_queries = [
        """
        SELECT 
            SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) as null_pickup_datetime,
            SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) as null_dropoff_datetime,
            SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END) as null_fare_amount
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        """,
        """
        SELECT 
            MIN(pickup_datetime) as min_pickup_date,
            MAX(pickup_datetime) as max_pickup_date,
            MIN(dropoff_datetime) as min_dropoff_date,
            MAX(dropoff_datetime) as max_dropoff_date
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        """,
        """
        SELECT COUNT(*) 
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        AND (
            fare_amount < 0 
            OR trip_distance < 0
            OR dropoff_datetime < pickup_datetime
        )
        """,
    ]

    validation_results = {}
    for query in validation_queries:
        result = pg_hook.get_records(
            query, parameters=(execution_date.strftime("%Y-%m-%d"),)
        )
        validation_results[query] = result

    logging.info(
        f"Validation results for {execution_date.strftime('%Y-%m')}: {validation_results}"
    )

    if validation_results[validation_queries[0]][0][0] > 0:
        raise ValueError("Found null values in critical fields")

    if validation_results[validation_queries[2]][0][0] > 0:
        raise ValueError("Found records violating business rules")

    return validation_results


with DAG(
    "nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Taxi Data Pipeline",
    schedule="0 0 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["nyc_taxi"],
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS taxi_trips_staging (
            id SERIAL PRIMARY KEY,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            pickup_latitude FLOAT,
            pickup_longitude FLOAT,
            dropoff_latitude FLOAT,
            dropoff_longitude FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            payment_type TEXT,
            rate_code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS taxi_data_processing_log (
            id SERIAL PRIMARY KEY,
            processing_date TIMESTAMP,
            year_month VARCHAR(7),
            records_processed INTEGER,
            status VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    load_data = PythonOperator(
        task_id="load_taxi_data", python_callable=load_taxi_data, provide_context=True
    )

    validate_loaded_data = PythonOperator(
        task_id="validate_data", python_callable=validate_data, provide_context=True
    )

    create_tables >> load_data >> validate_loaded_data
