from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10)
}

from google.cloud import bigquery
client = bigquery.Client()

def merge_stock_data():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.stock_data` AS
    WITH combined_stocks AS (
      SELECT *, 'AAPL' AS symbol FROM `is3107-451916.stock_dataset.AAPL`
      UNION ALL SELECT *, 'HIMS' AS symbol FROM `is3107-451916.stock_dataset.HIMS`
      UNION ALL SELECT *, 'MGNI' AS symbol FROM `is3107-451916.stock_dataset.MGNI`
      UNION ALL SELECT *, 'NVDA' AS symbol FROM `is3107-451916.stock_dataset.NVDA`
      UNION ALL SELECT *, 'RDFN' AS symbol FROM `is3107-451916.stock_dataset.RDFN`
      UNION ALL SELECT *, 'SOFI' AS symbol FROM `is3107-451916.stock_dataset.SOFI`
    )
    SELECT
      s.* EXCEPT(`MACD Signal`, `MACD Histogram`, `Stochastic K`, `Stochastic D`),
      `MACD Signal`    AS MACD_Signal,
      `MACD Histogram` AS MACD_Histogram,
      `Stochastic K`   AS Stochastic_K,
      `Stochastic D`   AS Stochastic_D,
      SAFE_CAST(f.sentiment_score AS FLOAT64)    AS sentiment_score,
      SAFE_CAST(f.news_count      AS INT64)      AS news_count,
      CAST(m.consumer_price_index  AS FLOAT64)   AS consumer_price_index,
      CAST(m.unemployment_rate     AS FLOAT64)   AS unemployment_rate,
      CAST(m.federal_funds_rate    AS FLOAT64)   AS federal_funds_rate
    FROM combined_stocks s
    LEFT JOIN `is3107-451916.sentiment.sentiment_data` f
      ON s.symbol = REPLACE(f.ticker, '.US', '') AND DATE(s.date) = DATE(f.date)
    LEFT JOIN `is3107-451916.macro_dataset.us_macro_indicators` m
      ON DATE(s.date) = DATE(m.date)
    """
    client.query(sql).result()
    print("📦 Merged stock_data updated.")



def generate_features_aapl():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_AAPL` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'AAPL'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for AAPL")



def generate_features_hims():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_HIMS` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'HIMS'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for HIMS")



def generate_features_mgni():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_MGNI` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'MGNI'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for MGNI")



def generate_features_nvda():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_NVDA` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'NVDA'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for NVDA")



def generate_features_rdfn():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_RDFN` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'RDFN'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for RDFN")



def generate_features_sofi():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.model_features_SOFI` AS
    SELECT *,
      CASE WHEN close >= prev_close THEN 1 ELSE -1 END AS price_direction
    FROM (
      SELECT
        date, symbol, close, volume,
        COALESCE(sentiment_score, LAST_VALUE(sentiment_score IGNORE NULLS)
          OVER (PARTITION BY symbol ORDER BY date), 0) AS sentiment_score,
        COALESCE(news_count, 0) AS news_count,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_day,
        AVG(close) OVER(PARTITION BY symbol ORDER BY date
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_avg_30_day,
        LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_2,
        LAG(close,3) OVER(PARTITION BY symbol ORDER BY date) AS prev_close_3,
        (close - LAG(close,1) OVER(PARTITION BY symbol ORDER BY date)) AS price_diff_1_day,
        CASE
          WHEN LAG(close,1) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,2) OVER(PARTITION BY symbol ORDER BY date)
           AND LAG(close,2) OVER(PARTITION BY symbol ORDER BY date) > LAG(close,3) OVER(PARTITION BY symbol ORDER BY date)
          THEN 1 ELSE -1 END AS trend_3_day,
        RSI, MACD, MACD_Signal, MACD_Histogram,
        Stochastic_K, Stochastic_D, ATR,
        Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
        (close - Middle_Band)/(Upper_Band-Lower_Band) AS band_position,
        CASE WHEN close>Upper_Band THEN 1 WHEN close<Lower_Band THEN -1 ELSE 0 END AS band_signal,
        SIGN(MACD_Histogram) AS macd_signal_direction,
        CASE WHEN MACD>0 AND MACD_Signal>0 THEN 1 WHEN MACD<0 AND MACD_Signal<0 THEN -1 ELSE 0 END AS macd_trend,
        CASE WHEN RSI>70 THEN 1 WHEN RSI<30 THEN -1 ELSE 0 END AS rsi_signal,
        CASE WHEN Stochastic_K>80 AND Stochastic_D>80 THEN 1
             WHEN Stochastic_K<20 AND Stochastic_D<20 THEN -1 ELSE 0 END AS stoch_signal,
        CASE WHEN EMA>SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) <= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN 1
             WHEN EMA<SMA AND LAG(EMA,1) OVER(PARTITION BY symbol ORDER BY date) >= LAG(SMA,1) OVER(PARTITION BY symbol ORDER BY date) THEN -1 ELSE 0 END AS ma_crossover,
        ATR/close AS relative_volatility
      FROM `is3107-451916.merged_data_6.stock_data`
      WHERE symbol = 'SOFI'
    )
    """
    client.query(sql).result()
    print("🔧 Features created for SOFI")



def train_model_aapl():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_AAPL`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_AAPL`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for AAPL")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_AAPL` AS
    SELECT
      'stock_direction_model_AAPL' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_AAPL`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_AAPL`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for AAPL")
    

    # Step 3: Predict next 7 days
    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_AAPL` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_AAPL`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      predicted_price_direction_probs
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_AAPL`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for AAPL")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_AAPL` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_AAPL`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for AAPL")



def train_model_hims():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_HIMS`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_HIMS`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for HIMS")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_HIMS` AS
    SELECT
      'stock_direction_model_HIMS' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_HIMS`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_HIMS`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for HIMS")

    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_HIMS` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_HIMS`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      predicted_price_direction_probs
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_HIMS`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for HIMS")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_HIMS` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_HIMS`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for HIMS")



def train_model_mgni():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_MGNI`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_MGNI`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for MGNI")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_MGNI` AS
    SELECT
      'stock_direction_model_MGNI' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_MGNI`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_MGNI`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for MGNI")

    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_MGNI` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_MGNI`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      predicted_price_direction_probs
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_MGNI`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for MGNI")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_MGNI` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_MGNI`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for MGNI")


def train_model_nvda():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_NVDA`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_NVDA`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for NVDA")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_NVDA` AS
    SELECT
      'stock_direction_model_NVDA' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_NVDA`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_NVDA`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for NVDA")

    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_NVDA` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_NVDA`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      predicted_price_direction_probs
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_NVDA`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for NVDA")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_NVDA` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_NVDA`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for NVDA")



def train_model_rdfn():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_RDFN`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_RDFN`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for RDFN")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_RDFN` AS
    SELECT
      'stock_direction_model_RDFN' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_RDFN`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_RDFN`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for RDFN")

    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_RDFN` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_RDFN`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      predicted_price_direction_probs
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_RDFN`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for RDFN")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_RDFN` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_RDFN`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for RDFN")


def train_model_sofi():
    from google.cloud import bigquery
    client = bigquery.Client()

    # Step 1: Train model
    train_sql = f"""
    CREATE OR REPLACE MODEL `is3107-451916.merged_data_6.stock_direction_model_SOFI`
    OPTIONS(
      MODEL_TYPE='LOGISTIC_REG',
      INPUT_LABEL_COLS=['price_direction'],
      AUTO_CLASS_WEIGHTS=TRUE,
      DATA_SPLIT_METHOD='CUSTOM',
      DATA_SPLIT_COL='is_training_data'
    ) AS
    WITH FeatureDataWithSplit AS (
      SELECT *,
        CASE
          WHEN date <= '2025-01-30' THEN TRUE
          WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
          ELSE NULL
        END AS is_training_data
      FROM `is3107-451916.merged_data_6.model_features_SOFI`
      WHERE price_direction IS NOT NULL
    )
    SELECT
      date, symbol, close, volume, sentiment_score, news_count,
      moving_avg_7_day, moving_avg_30_day,
      prev_close, prev_close_2, prev_close_3,
      price_diff_1_day, trend_3_day,
      RSI, MACD, MACD_Signal, MACD_Histogram,
      Stochastic_K, Stochastic_D,
      ATR, Lower_Band, Middle_Band, Upper_Band, SMA, EMA,
      band_position, band_signal, macd_signal_direction,
      macd_trend, rsi_signal, stoch_signal, ma_crossover,
      relative_volatility, price_direction, is_training_data
    FROM FeatureDataWithSplit
    WHERE is_training_data IS NOT NULL
    """
    client.query(train_sql).result()
    print("✅ Trained model for SOFI")

    # Step 2: Evaluate (final fixed logic)
    eval_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.eval_results_SOFI` AS
    SELECT
      'stock_direction_model_SOFI' AS model_id,
      CURRENT_TIMESTAMP() AS evaluation_time,
      *
    FROM
      ML.EVALUATE(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_SOFI`,
        (
          SELECT *
          FROM (
            SELECT *,
              CASE
                WHEN date <= '2025-01-30' THEN TRUE
                WHEN date > '2025-01-30' AND date <= '2025-02-28' THEN FALSE
                ELSE NULL
              END AS is_training_data
            FROM `is3107-451916.merged_data_6.model_features_SOFI`
            WHERE price_direction IS NOT NULL
          )
          WHERE is_training_data = FALSE
        )
      )
    """
    client.query(eval_sql).result()
    print("📊 Evaluation stored for SOFI")

    predict_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_weekly_SOFI` AS
    WITH latest_week AS (
      SELECT
        * EXCEPT(price_direction)
      FROM
        `is3107-451916.merged_data_6.model_features_SOFI`
      ORDER BY
        date DESC
      LIMIT 7
    )
    SELECT
      date,
      predicted_price_direction,
      (SELECT prob FROM UNNEST(predicted_price_direction_probs)
   WHERE label = predicted_price_direction) AS predicted_prob
    FROM
      ML.PREDICT(
        MODEL `is3107-451916.merged_data_6.stock_direction_model_SOFI`,
        (SELECT * FROM latest_week)
      )
    """
    client.query(predict_sql).result()
    print("📈 7-day prediction stored for SOFI")

    aggregate_sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.weekly_direction_SOFI` AS
    SELECT
      IF(SUM(predicted_price_direction) > 0, 1, -1) AS predicted_weekly_direction
    FROM
      `is3107-451916.merged_data_6.predictions_weekly_SOFI`
    """
    client.query(aggregate_sql).result()
    print("🔄 Weekly direction aggregated for SOFI")

def combine_predictions_all():
    from google.cloud import bigquery
    client = bigquery.Client()
    sql = f"""
    CREATE OR REPLACE TABLE `is3107-451916.merged_data_6.predictions_all` AS
    WITH next_week_dates AS (
    SELECT
        DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 7 DAY) AS start_date,
        DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 11 DAY) AS end_date
    )

    SELECT 'AAPL' AS symbol, predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down') AS direction_label,
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_AAPL`, next_week_dates AS d

    UNION ALL
    SELECT 'HIMS', predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down'),
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_HIMS`, next_week_dates AS d

    UNION ALL
    SELECT 'MGNI', predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down'),
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_MGNI`, next_week_dates AS d

    UNION ALL
    SELECT 'NVDA', predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down'),
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_NVDA`, next_week_dates AS d

    UNION ALL
    SELECT 'RDFN', predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down'),
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_RDFN`, next_week_dates AS d

    UNION ALL
    SELECT 'SOFI', predicted_weekly_direction,
        IF(predicted_weekly_direction = 1, 'up', 'down'),
        d.start_date, d.end_date
    FROM `is3107-451916.merged_data_6.weekly_direction_SOFI`, next_week_dates AS d;
    """
    client.query(sql).result()
    print("🧩 Combined predictions saved to predictions_all")


with DAG("stock_direction_model_pipeline",
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False,
         tags=["bigquery", "training", "feature_engineering", "prediction"]) as dag:
    
    t_merge = PythonOperator(task_id='merge_stock_data', python_callable=merge_stock_data)
    t_feat_aapl = PythonOperator(task_id='generate_features_aapl', python_callable=generate_features_aapl)
    t_feat_hims = PythonOperator(task_id='generate_features_hims', python_callable=generate_features_hims)
    t_feat_mgni = PythonOperator(task_id='generate_features_mgni', python_callable=generate_features_mgni)
    t_feat_nvda = PythonOperator(task_id='generate_features_nvda', python_callable=generate_features_nvda)
    t_feat_rdfn = PythonOperator(task_id='generate_features_rdfn', python_callable=generate_features_rdfn)
    t_feat_sofi = PythonOperator(task_id='generate_features_sofi', python_callable=generate_features_sofi)
    # train, predict, and aggregate
    t_train_aapl = PythonOperator(task_id='train_model_aapl', python_callable=train_model_aapl)
    t_train_hims = PythonOperator(task_id='train_model_hims', python_callable=train_model_hims)
    t_train_mgni = PythonOperator(task_id='train_model_mgni', python_callable=train_model_mgni)
    t_train_nvda = PythonOperator(task_id='train_model_nvda', python_callable=train_model_nvda)
    t_train_rdfn = PythonOperator(task_id='train_model_rdfn', python_callable=train_model_rdfn)
    t_train_sofi = PythonOperator(task_id='train_model_sofi', python_callable=train_model_sofi)
    t_combine_preds = PythonOperator(task_id='combine_predictions_all', python_callable=combine_predictions_all)

    t_merge >> t_feat_aapl >> t_train_aapl
    t_merge >> t_feat_hims >> t_train_hims
    t_merge >> t_feat_mgni >> t_train_mgni
    t_merge >> t_feat_nvda >> t_train_nvda
    t_merge >> t_feat_rdfn >> t_train_rdfn
    t_merge >> t_feat_sofi >> t_train_sofi
    [t_train_aapl, t_train_hims, t_train_mgni, t_train_nvda, t_train_rdfn, t_train_sofi] >> t_combine_preds
