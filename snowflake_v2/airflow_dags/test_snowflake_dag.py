from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="snowflake_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once", 
    catchup=False,
) as dag:

    test_query = SnowflakeOperator(
        task_id="run_snowflake_query",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT CURRENT_TIMESTAMP();"
    )

