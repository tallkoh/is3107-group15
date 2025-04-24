from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

def load_sql(filename):
    with open(f"/home/airflow/gcs/dags/sql/{filename}", "r") as file:
        return file.read()
    
default_args = {
    'start_date': datetime(2025, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('feature_pipeline_dag',
         default_args=default_args,
         schedule_interval=None, 
         catchup=False,
         description='Runs merge, price feature engineering, and direction feature engineering') as dag:

    merge_data = BigQueryInsertJobOperator(
        task_id='merge_data',
        configuration={
            "query": {
                "query": load_sql("merge_data.sql"),
                "useLegacySql": False
            }
        },
        location="US"
    )

    feature_price = BigQueryInsertJobOperator(
        task_id='feature_engineering_price',
        configuration={
            "query": {
                "query": load_sql("feature_engineering_price.sql"),
                "useLegacySql": False
            }
        },
        location="US"
    )

    feature_direction = BigQueryInsertJobOperator(
        task_id='feature_engineering_direction',
        configuration={
            "query": {
                "query": load_sql("feature_engineering_direction.sql"),
                "useLegacySql": False
            }
        },
        location="US"
    )

    merge_data >> [feature_price, feature_direction]
