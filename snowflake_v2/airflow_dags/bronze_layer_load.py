from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Snowflake connection configured in Airflow UI
SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "chathumi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="bronze_layer_load",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["snowflake", "bronze", "etl"]
) as dag:

    load_bronze = SnowflakeOperator(
        task_id="run_load_bronze_procedure",
        sql="CALL DATAWAREHOUSE.BRONZE.LOAD_BRONZE();",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="COMPUTE_WH",   # Correct warehouse
        database="DATAWAREHOUSE",
        schema="BRONZE",
        role="ACCOUNTADMIN"
    )

    load_bronze

