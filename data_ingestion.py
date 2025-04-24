from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 4, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('data_ingestion',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         description='Daily ingestion of API data (sentiment + historical), triggers feature pipeline',
         ) as dag:

    ingest_sentiment = HttpOperator(
        task_id='ingest_sentiment',
        http_conn_id='sentiment_api_conn',
        endpoint='sentiment-ingestion',
        method='GET', 
        log_response=True
    )

    ingest_historical = HttpOperator(
        task_id='ingest_historical',
        http_conn_id='historical_api_conn',
        endpoint='historicaldatainjection',
        method='GET',
        log_response=True
    )

    trigger_feature_pipeline = TriggerDagRunOperator(
        task_id='trigger_feature_pipeline_dag',
        trigger_dag_id='feature_pipeline_dag',
        wait_for_completion=False
    )

    [ingest_sentiment, ingest_historical] >> trigger_feature_pipeline
