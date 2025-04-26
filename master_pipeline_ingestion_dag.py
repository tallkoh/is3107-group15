from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
import pendulum

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

def is_first_day_of_month(execution_date_str, **kwargs):
    execution_date = pendulum.parse(execution_date_str)
    return execution_date.day == 1

with DAG(
    dag_id="master_pipeline",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # Every day at 8AM UTC
    catchup=False,
    tags=["regiflow", "meta", "pipeline"]
) as dag:

    trigger_historical = TriggerDagRunOperator(
        task_id="trigger_daily_historical",
        trigger_dag_id="daily_historical_ingestion_etl", 
        execution_date="{{ ts }}",
        wait_for_completion=True,
        reset_dag_run=False
    )


    trigger_sentiment = TriggerDagRunOperator(
        task_id="trigger_daily_sentiment",
        trigger_dag_id="daily_sentiment_etl",
        execution_date="{{ ts }}",
        wait_for_completion=True,
        reset_dag_run=False
    )

    check_first_day = ShortCircuitOperator(
        task_id="is_first_day_of_month",
        python_callable=is_first_day_of_month,
        op_args=["{{ ds }}"]
    )

    trigger_macro = TriggerDagRunOperator(
        task_id="trigger_monthly_macro",
        trigger_dag_id="macro_monthly_ingestion",
        execution_date="{{ ts }}",
        wait_for_completion=True,
        reset_dag_run=False
    )

    [trigger_historical, trigger_sentiment] >> check_first_day >> trigger_macro