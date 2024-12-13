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
from sqlalchemy import create_engine


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
    execution_date = context["logical_date"].replace(tzinfo=None)
    query_date = execution_date.replace(year=2022)

    # 直接使用固定的連接資訊
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

    start_of_month = f"{query_date.year}-{query_date.month:02d}-01T00:00:00.000"
    if query_date.month == 12:
        next_month = f"{query_date.year + 1}-01-01T00:00:00.000"
    else:
        next_month = f"{query_date.year}-{query_date.month + 1:02d}-01T00:00:00.000"

    base_url = "https://data.cityofnewyork.us/resource/qp3b-zxtp.json"
    where_clause = (
        f"tpep_pickup_datetime >= '{start_of_month}' "
        f"AND tpep_pickup_datetime < '{next_month}'"
    )

    offset = 0
    batch_size = 1000
    total_records = 0

    while True:
        params = {"$limit": batch_size, "$offset": offset, "$where": where_clause}

        logging.info(f"Fetching data with params: {params}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        records = response.json()
        if not records:
            break

        df = pd.DataFrame(records)

        # 正確轉換資料類型
        df["vendorid"] = pd.to_numeric(df["vendorid"], errors="coerce").astype("Int64")
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        df["passenger_count"] = pd.to_numeric(
            df["passenger_count"], errors="coerce"
        ).astype("Int64")
        df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
        df["ratecodeid"] = pd.to_numeric(df["ratecodeid"], errors="coerce").astype(
            "Int64"
        )
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str)
        df["pulocationid"] = pd.to_numeric(df["pulocationid"], errors="coerce").astype(
            "Int64"
        )
        df["dolocationid"] = pd.to_numeric(df["dolocationid"], errors="coerce").astype(
            "Int64"
        )
        df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").astype(
            "Int64"
        )
        df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
        df["extra"] = pd.to_numeric(df["extra"], errors="coerce")
        df["mta_tax"] = pd.to_numeric(df["mta_tax"], errors="coerce")
        df["tip_amount"] = pd.to_numeric(df["tip_amount"], errors="coerce")
        df["tolls_amount"] = pd.to_numeric(df["tolls_amount"], errors="coerce")
        df["improvement_surcharge"] = pd.to_numeric(
            df["improvement_surcharge"], errors="coerce"
        )
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
        df["congestion_surcharge"] = pd.to_numeric(
            df["congestion_surcharge"], errors="coerce"
        )
        df["airport_fee"] = pd.to_numeric(df["airport_fee"], errors="coerce")

        df.to_sql(
            "taxi_trips_staging",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        total_records += len(df)
        offset += batch_size
        logging.info(f"Loaded {len(df)} records")

    # 寫入處理日誌
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    log_sql = """
    INSERT INTO taxi_data_processing_log (
        processing_date, year_month, records_processed, status
    ) VALUES (%s, %s, %s, 'completed')
    """
    pg_hook.run(
        log_sql,
        parameters=(datetime.now(), query_date.strftime("%Y-%m"), total_records),
    )


@error_handling_wrapper
def validate_data(**context):
    """執行數據質量檢查"""
    execution_date = context["logical_date"].replace(tzinfo=None)
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    validation_queries = [
        """
        SELECT 
            COALESCE(SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END), 0) as null_pickup_datetime,
            COALESCE(SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END), 0) as null_dropoff_datetime,
            COALESCE(SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END), 0) as null_fare_amount
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', tpep_pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        """,
        """
        SELECT 
            MIN(tpep_pickup_datetime) as min_pickup_date,
            MAX(tpep_pickup_datetime) as max_pickup_date,
            MIN(tpep_dropoff_datetime) as min_dropoff_date,
            MAX(tpep_dropoff_datetime) as max_dropoff_date
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', tpep_pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        """,
        """
        SELECT COALESCE(COUNT(*), 0)
        FROM taxi_trips_staging
        WHERE DATE_TRUNC('month', tpep_pickup_datetime AT TIME ZONE 'UTC') = 
              DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
        AND (
            fare_amount < 0 
            OR trip_distance < 0
            OR tpep_dropoff_datetime < tpep_pickup_datetime
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

    # 檢查是否有數據
    data_exists_query = """
    SELECT COUNT(*) 
    FROM taxi_trips_staging
    WHERE DATE_TRUNC('month', tpep_pickup_datetime AT TIME ZONE 'UTC') = 
          DATE_TRUNC('month', %s::timestamp AT TIME ZONE 'UTC')
    """
    record_count = pg_hook.get_first(
        data_exists_query, parameters=(execution_date.strftime("%Y-%m-%d"),)
    )[0]

    if record_count == 0:
        logging.warning(f"No data found for {execution_date.strftime('%Y-%m')}")
        return validation_results

    # 如果有數據，則進行驗證
    null_counts = validation_results[validation_queries[0]][0]
    if any(count > 0 for count in null_counts if count is not None):
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
        DROP TABLE IF EXISTS taxi_trips_staging;
        DROP TABLE IF EXISTS taxi_data_processing_log;
        
        CREATE TABLE IF NOT EXISTS taxi_trips_staging (
            id SERIAL PRIMARY KEY,
            vendorid INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count FLOAT,
            trip_distance FLOAT,
            ratecodeid INTEGER,
            store_and_fwd_flag TEXT,
            pulocationid INTEGER,
            dolocationid INTEGER,
            payment_type INTEGER,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
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
