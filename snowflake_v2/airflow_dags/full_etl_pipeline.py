from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    "full_etl_pipeline",
    start_date=datetime(2025, 11, 25),
    schedule_interval="@daily",
    catchup=False
):

    load_bronze = TriggerDagRunOperator(
        task_id="run_bronze",
        trigger_dag_id="bronze_layer_load"
    )

    load_silver = TriggerDagRunOperator(
        task_id="run_silver",
        trigger_dag_id="silver_layer_load"
    )

    load_gold = TriggerDagRunOperator(
        task_id="run_gold",
        trigger_dag_id="gold_layer_load"
    )

    load_bronze >> load_silver >> load_gold

