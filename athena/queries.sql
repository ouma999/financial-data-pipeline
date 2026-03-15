-- ─────────────────────────────────────────────────────────────────────────────
-- Athena SQL Queries — Financial Data Pipeline
-- Run these in AWS Athena after the Glue ETL job completes
-- ─────────────────────────────────────────────────────────────────────────────


-- ── 1. Create Database ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS financial_data;


-- ── 2. Create External Table pointing to S3 Processed Zone ───────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS financial_data.stock_prices (
    ticker          STRING,
    timestamp       TIMESTAMP,
    date            DATE,
    hour            INT,
    day_of_week     INT,
    week_of_year    INT,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          BIGINT,
    price_change    DOUBLE,
    price_change_pct DOUBLE,
    intraday_range  DOUBLE,
    ma_7            DOUBLE,
    ma_14           DOUBLE,
    ma_30           DOUBLE,
    volatility_7d   DOUBLE,
    above_ma_7      INT,
    above_ma_30     INT,
    volume_spike    INT,
    value_at_risk_1d DOUBLE,
    risk_tier       STRING,
    market_cap      BIGINT,
    pe_ratio        DOUBLE,
    sector          STRING,
    industry        STRING,
    fifty_two_week_high DOUBLE,
    fifty_two_week_low  DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://YOUR-PROCESSED-BUCKET/processed/stock_prices/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Load partitions
MSCK REPAIR TABLE financial_data.stock_prices;


-- ── 3. Basic Analytics Queries ────────────────────────────────────────────────

-- Latest price for each ticker
SELECT
    ticker,
    MAX(timestamp) AS latest_timestamp,
    close AS latest_price,
    price_change_pct AS daily_change_pct,
    risk_tier,
    sector
FROM financial_data.stock_prices
WHERE year = 2026 AND month = 3
GROUP BY ticker, close, price_change_pct, risk_tier, sector
ORDER BY ticker;


-- Top 5 biggest movers today
SELECT
    ticker,
    close,
    price_change,
    price_change_pct,
    volume,
    sector
FROM financial_data.stock_prices
WHERE date = CURRENT_DATE
ORDER BY ABS(price_change_pct) DESC
LIMIT 10;


-- Average daily volume by sector
SELECT
    sector,
    COUNT(DISTINCT ticker) AS num_stocks,
    ROUND(AVG(volume), 0) AS avg_volume,
    ROUND(AVG(close), 2) AS avg_price,
    ROUND(AVG(volatility_7d), 4) AS avg_volatility
FROM financial_data.stock_prices
WHERE year = 2026
GROUP BY sector
ORDER BY avg_volatility DESC;


-- ── 4. Risk Analysis Queries ──────────────────────────────────────────────────

-- Portfolio risk summary
SELECT
    ticker,
    sector,
    ROUND(AVG(close), 2) AS avg_price,
    ROUND(AVG(volatility_7d), 4) AS avg_volatility,
    ROUND(MAX(value_at_risk_1d), 2) AS max_var_1d,
    risk_tier,
    COUNT(*) AS data_points
FROM financial_data.stock_prices
WHERE year = 2026
GROUP BY ticker, sector, risk_tier
ORDER BY avg_volatility DESC;


-- High risk alerts — stocks with elevated volatility
SELECT
    ticker,
    date,
    close,
    volatility_7d,
    value_at_risk_1d,
    volume,
    volume_spike
FROM financial_data.stock_prices
WHERE risk_tier IN ('ELEVATED', 'HIGH')
AND year = 2026
ORDER BY volatility_7d DESC;


-- ── 5. Technical Analysis Queries ────────────────────────────────────────────

-- Golden cross signals (MA7 crosses above MA30)
SELECT
    ticker,
    date,
    close,
    ma_7,
    ma_30,
    above_ma_7,
    above_ma_30,
    CASE
        WHEN above_ma_7 = 1 AND above_ma_30 = 1 THEN 'BULLISH'
        WHEN above_ma_7 = 0 AND above_ma_30 = 0 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS signal
FROM financial_data.stock_prices
WHERE year = 2026
ORDER BY ticker, date;


-- Volume spike detection
SELECT
    ticker,
    timestamp,
    close,
    volume,
    vol_ma_7,
    ROUND(CAST(volume AS DOUBLE) / NULLIF(vol_ma_7, 0) * 100, 1) AS volume_vs_avg_pct,
    price_change_pct
FROM financial_data.stock_prices
WHERE volume_spike = 1
AND year = 2026
ORDER BY volume_vs_avg_pct DESC
LIMIT 20;


-- ── 6. Performance & Cost Optimization Queries ───────────────────────────────

-- Check partition efficiency (always filter by year/month!)
SELECT
    year,
    month,
    COUNT(*) AS records,
    COUNT(DISTINCT ticker) AS unique_tickers
FROM financial_data.stock_prices
GROUP BY year, month
ORDER BY year, month;


-- Weekly return by ticker
SELECT
    ticker,
    week_of_year,
    year,
    MIN(close) AS week_low,
    MAX(close) AS week_high,
    FIRST_VALUE(close) OVER (
        PARTITION BY ticker, week_of_year
        ORDER BY timestamp
    ) AS week_open,
    LAST_VALUE(close) OVER (
        PARTITION BY ticker, week_of_year
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS week_close,
    SUM(volume) AS total_volume
FROM financial_data.stock_prices
WHERE year = 2026
GROUP BY ticker, week_of_year, year, close, timestamp
ORDER BY ticker, week_of_year;
