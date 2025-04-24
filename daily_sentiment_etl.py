import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime, date
from airflow.decorators import dag, task
from google.cloud import bigquery

load_dotenv()
PROJECT_ID = "is3107-451916"
DATASET = "sentiment"
TABLE = "sentiment_data"
API_URL = "https://eodhd.com/api/sentiments"
API_TOKEN = os.getenv("EODHD_API_KEY")
TICKERS = ["AAPL.US", "NVDA.US", "SOFI.US", "HIMS.US", "RDFN.US", "MGNI.US"]

@dag(
    dag_id="daily_sentiment_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sentiment", "bq", "etl"]
)
def daily_sentiment_pipeline():

    @task
    def extract():
        print("Extracting sentiment data from API...")

        today = date.today()
        start_date = end_date = today.strftime("%Y-%m-%d")

        params = {
            "s": ",".join(TICKERS),
            "from": start_date,
            "to": end_date,
            "api_token": API_TOKEN,
            "fmt": "json"
        }

        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        data = response.json()

        if isinstance(data, list):
            print("⚠️ API returned a flat list, no sentiment data available.")
            return {}  
        
        if not isinstance(data, dict):
            raise ValueError("❌ Unexpected API format: not dict or list")
        
        return data


    @task
    def transform(sentiment_data):
        print("Transforming data...")

        if not sentiment_data:
            print("⚠️ No sentiment data to transform.")
            return []

        processed = []
        for ticker, records in sentiment_data.items():
            for record in records:
                processed.append({
                    "ticker": ticker,
                    "date": record.get("date"),
                    "news_count": record.get("count", 0),
                    "sentiment_score": str(record.get("normalized", ""))
                })

        print(f"Processed {len(processed)} records.")
        return processed


    @task
    def load(processed_data):
        print("Loading into BigQuery...")

        if not processed_data:
            print("⚠️ No processed data to load. Skipping BigQuery insert.")
            return

        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        errors = client.insert_rows_json(table_id, processed_data)
        if errors:
            print(f"❌ BigQuery insert errors: {errors}")
            raise Exception("BigQuery insert failed.")
        else:
            print(f"✅ Successfully inserted {len(processed_data)} rows to {table_id}.")


    # Task flow
    raw_data = extract()
    cleaned_data = transform(raw_data)
    load(cleaned_data)

dag = daily_sentiment_pipeline()