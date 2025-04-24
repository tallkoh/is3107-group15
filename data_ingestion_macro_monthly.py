from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('data_ingestion_macro_monthly',
         default_args=default_args,
         schedule_interval='@monthly',
         catchup=False,
         description='Monthly ingestion of macroeconomic data',
         ) as dag:

    ingest_macro = HttpOperator(
        task_id='ingest_macro',
        http_conn_id='macro_api_conn',
        endpoint='macroeconomicsdatainjection',
        method='POST',
        log_response=True
    )
