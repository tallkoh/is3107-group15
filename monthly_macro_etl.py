from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import numpy as np
import requests

# === Config ===
PROJECT_ID = "is3107-451916"
DATASET = "macro_data"
TABLE = "us_macro_indicators"
API_KEY = "FQ24V0N6K7A843UN"

INDICATORS = {
    "CPI": "consumer_price_index",
    "UNEMPLOYMENT": "unemployment_rate",
    "FEDERAL_FUNDS_RATE": "federal_funds_rate"
}

@dag(
    dag_id="monthly_macro_ingestion_etl",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["macro", "etl"]
)
def macro_etl_pipeline():

    @task
    def extract():
        def fetch_latest(indicator):
            url = f"https://www.alphavantage.co/query?function={indicator}&apikey={API_KEY}"
            response = requests.get(url)
            data = response.json()

            if "data" not in data:
                print(f" No data for {indicator}")
                return pd.DataFrame()

            df = pd.DataFrame(data["data"])
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date", ascending=False).dropna(subset=["date"])

            if df.empty:
                return pd.DataFrame()

            latest_date = df.iloc[0]["date"]
            df = df[df["date"] == latest_date]
            df = df.rename(columns={"value": INDICATORS[indicator]})
            df[INDICATORS[indicator]] = pd.to_numeric(df[INDICATORS[indicator]], errors="coerce")

            return df[["date", INDICATORS[indicator]]]

        combined_df = pd.DataFrame()

        for indicator in INDICATORS:
            df = fetch_latest(indicator)
            if df.empty:
                continue
            combined_df = df if combined_df.empty else pd.merge(combined_df, df, on="date", how="outer")

        if combined_df.empty:
            raise ValueError(" No data retrieved from any indicator.")

        return combined_df.to_json()  # return raw JSON to be transformed

    @task
    def transform(raw_json):
        df = pd.read_json(raw_json)
        df.rename(columns={"date": "Date"}, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
        df = df.replace({np.nan: None})
        return df.to_dict(orient="records")

    @task
    def load(rows):
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        latest_date = rows[0]["Date"]

        # Check if the row already exists
        query = f"""
            SELECT 1 FROM `{table_id}`
            WHERE Date = DATE('{latest_date}')
            LIMIT 1
        """
        result_df = client.query(query).to_dataframe()

        if not result_df.empty:
            print(f" Data for {latest_date} already exists. Skipping insert.")
            return

        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("consumer_price_index", "FLOAT"),
            bigquery.SchemaField("unemployment_rate", "FLOAT"),
            bigquery.SchemaField("federal_funds_rate", "FLOAT")
        ]

        try:
            client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            print(f" Created table: {TABLE}")

        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f" BigQuery insert failed: {errors}")
        else:
            print(f" Inserted data for {latest_date}")

    # DAG flow
    raw = extract()
    transformed = transform(raw)
    load(transformed)

dag = macro_etl_pipeline()