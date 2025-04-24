import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import numpy as np

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
PROJECT_ID = "is3107-451916"
DATASET_NAME = "stock_data"
STOCK_SYMBOLS = ["AAPL", "NVDA", "SOFI", "HIMS", "RDFN", "MGNI"]
DEFAULT_START_DATE = pd.to_datetime("2024-01-01").date()
    

@dag(
    dag_id="daily_historical_ingestion_etl",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stocks", "etl"]
)
def historical_pipeline():

    @task
    def extract():
        ts = TimeSeries(key=API_KEY, output_format="pandas")
        ti = TechIndicators(key=API_KEY, output_format="pandas")

        extracted = []

        for symbol in STOCK_SYMBOLS:
            try:
                print(f" Extracting {symbol}")
                price_data, _ = ts.get_daily(symbol=symbol, outputsize="full")
                price_data.index = pd.to_datetime(price_data.index)
                price_data = price_data.sort_index()

                # Indicators
                rsi = ti.get_rsi(symbol=symbol, interval="daily", time_period=14, series_type="close")[0]
                macd = ti.get_macd(symbol=symbol, interval="daily", series_type="close")[0]
                stoch = ti.get_stoch(symbol=symbol, interval="daily")[0]
                atr = ti.get_atr(symbol=symbol, interval="daily", time_period=14)[0]
                bbands = ti.get_bbands(symbol=symbol, interval="daily", time_period=20, series_type="close")[0]
                sma = ti.get_sma(symbol=symbol, interval="daily", time_period=50, series_type="close")[0]
                ema = ti.get_ema(symbol=symbol, interval="daily", time_period=50, series_type="close")[0]

                # Store everything in dictionary
                extracted.append({
                    "symbol": symbol,
                    "price": price_data.to_json(),
                    "rsi": rsi.to_json(),
                    "macd": macd.to_json(),
                    "stoch": stoch.to_json(),
                    "atr": atr.to_json(),
                    "bbands": bbands.to_json(),
                    "sma": sma.to_json(),
                    "ema": ema.to_json()
                })
            except Exception as e:
                print(f" Failed for {symbol}: {e}")

        return extracted

    @task
    def transform(extracted_data):
        merged_data = {}
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()

        for stock in extracted_data:
            symbol = stock["symbol"]
            try:
                price = pd.read_json(stock["price"])
                price = price.rename(columns={
                    "1. open": "Open", "2. high": "High", "3. low": "Low",
                    "4. close": "Close", "5. volume": "Volume"
                })

                rsi = pd.read_json(stock["rsi"]).rename(columns={"RSI": "RSI"})
                macd = pd.read_json(stock["macd"]).rename(columns={
                    "MACD": "MACD", "MACD_Signal": "MACD Signal", "MACD_Hist": "MACD Histogram"
                })
                stoch = pd.read_json(stock["stoch"]).rename(columns={
                    "SlowK": "Stochastic K", "SlowD": "Stochastic D"
                })
                atr = pd.read_json(stock["atr"]).rename(columns={"ATR": "ATR"})
                bbands = pd.read_json(stock["bbands"]).rename(columns={
                    "Real Lower Band": "Lower_Band",
                    "Real Middle Band": "Middle_Band",
                    "Real Upper Band": "Upper_Band"
                })
                sma = pd.read_json(stock["sma"]).rename(columns={"SMA": "SMA"})
                ema = pd.read_json(stock["ema"]).rename(columns={"EMA": "EMA"})

                # Merge indicators
                merged = price
                for df in [rsi, macd, stoch, atr, bbands, sma, ema]:
                    merged = pd.merge(merged, df, how="inner", left_index=True, right_index=True)

                merged = merged.reset_index().rename(columns={"index": "date"})
                merged["date"] = pd.to_datetime(merged["date"]).dt.date

                # ✅ Filter to just yesterday's date
                filtered = merged[merged["date"] == yesterday]

                print(f"📅 {symbol}: Filtering for {yesterday}, Rows found: {len(filtered)}")

                if not filtered.empty:
                    filtered["date"] = filtered["date"].astype(str)
                    merged_data[symbol] = filtered.to_dict(orient="records")
                else:
                    print(f"⚠️ {symbol}: No data available for {yesterday}")

            except Exception as e:
                print(f"❌ Error transforming {symbol}: {e}")

        return merged_data


    @task
    def load(merged_data):
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DATASET_NAME)

        # Ensure dataset exists
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            client.create_dataset(bigquery.Dataset(dataset_ref))
            print(f"✅ Created dataset: {DATASET_NAME}")

        # Table schema definition
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("Open", "FLOAT"),
            bigquery.SchemaField("High", "FLOAT"),
            bigquery.SchemaField("Low", "FLOAT"),
            bigquery.SchemaField("Close", "FLOAT"),
            bigquery.SchemaField("Volume", "INTEGER"),
            bigquery.SchemaField("RSI", "FLOAT"),
            bigquery.SchemaField("MACD", "FLOAT"),
            bigquery.SchemaField("MACD Signal", "FLOAT"),
            bigquery.SchemaField("MACD Histogram", "FLOAT"),
            bigquery.SchemaField("Stochastic K", "FLOAT"),
            bigquery.SchemaField("Stochastic D", "FLOAT"),
            bigquery.SchemaField("ATR", "FLOAT"),
            bigquery.SchemaField("Lower_Band", "FLOAT"),
            bigquery.SchemaField("Middle_Band", "FLOAT"),
            bigquery.SchemaField("Upper_Band", "FLOAT"),
            bigquery.SchemaField("SMA", "FLOAT"),
            bigquery.SchemaField("EMA", "FLOAT"),
        ]

        for symbol, rows in merged_data.items():
            table_ref = dataset_ref.table(symbol)

            # Always ensure table exists
            try:
                client.get_table(table_ref)
            except NotFound:
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                print(f"✅ Created table: {symbol}")

            print(f" Processing {symbol} with {len(rows)} rows")

            if not rows:
                print(f" No data to insert for {symbol} — but table has been created.")
                continue

            errors = client.insert_rows_json(table_ref, rows)
            if not errors:
                print(f"Inserted {len(rows)} rows into {symbol}")
            else:
                print(f" Insert errors for {symbol}: {errors}")

    # Task flow
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

dag = historical_pipeline()
