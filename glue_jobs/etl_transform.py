"""
AWS Glue ETL Job — Financial Data Pipeline
Reads raw JSON from S3, cleans and transforms it,
adds technical indicators, and writes Parquet to processed zone
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Get job parameters ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "DEST_BUCKET",
    "SOURCE_PREFIX",
    "DEST_PREFIX"
])

# ── Initialize Glue + Spark ───────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"Starting Glue job: {args['JOB_NAME']}")
logger.info(f"Source: s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}")
logger.info(f"Dest:   s3://{args['DEST_BUCKET']}/{args['DEST_PREFIX']}")


# ── Step 1: Read raw JSON from S3 ─────────────────────────────────────────────
logger.info("Step 1: Reading raw data from S3...")

raw_df = spark.read.json(
    f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}/"
)

logger.info(f"Raw records loaded: {raw_df.count()}")
raw_df.printSchema()


# ── Step 2: Data Quality Checks ───────────────────────────────────────────────
logger.info("Step 2: Running data quality checks...")

# Drop records missing critical fields
clean_df = raw_df.dropna(subset=["ticker", "close", "timestamp", "volume"])

# Remove records with zero or negative prices
clean_df = clean_df.filter(
    (F.col("close") > 0) &
    (F.col("open") > 0) &
    (F.col("volume") > 0)
)

# Remove duplicates
clean_df = clean_df.dropDuplicates(["ticker", "timestamp"])

records_after_quality = clean_df.count()
logger.info(f"Records after quality checks: {records_after_quality}")


# ── Step 3: Type Casting & Standardization ────────────────────────────────────
logger.info("Step 3: Casting types and standardizing fields...")

clean_df = clean_df \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("close", F.col("close").cast(DoubleType())) \
    .withColumn("open", F.col("open").cast(DoubleType())) \
    .withColumn("high", F.col("high").cast(DoubleType())) \
    .withColumn("low", F.col("low").cast(DoubleType())) \
    .withColumn("volume", F.col("volume").cast(LongType())) \
    .withColumn("market_cap", F.col("market_cap").cast(LongType())) \
    .withColumn("ticker", F.upper(F.trim(F.col("ticker")))) \
    .withColumn("sector", F.initcap(F.col("sector"))) \
    .withColumn("date", F.to_date("timestamp")) \
    .withColumn("hour", F.hour("timestamp")) \
    .withColumn("day_of_week", F.dayofweek("timestamp")) \
    .withColumn("week_of_year", F.weekofyear("timestamp")) \
    .withColumn("month", F.month("timestamp")) \
    .withColumn("year", F.year("timestamp"))


# ── Step 4: Technical Indicators ──────────────────────────────────────────────
logger.info("Step 4: Calculating technical indicators...")

# Window specs for time-series calculations per ticker
window_ticker = Window.partitionBy("ticker").orderBy("timestamp")
window_ticker_7d = Window.partitionBy("ticker").orderBy("timestamp").rowsBetween(-6, 0)
window_ticker_14d = Window.partitionBy("ticker").orderBy("timestamp").rowsBetween(-13, 0)
window_ticker_30d = Window.partitionBy("ticker").orderBy("timestamp").rowsBetween(-29, 0)

clean_df = clean_df \
    .withColumn("prev_close", F.lag("close", 1).over(window_ticker)) \
    .withColumn("price_change", F.col("close") - F.col("prev_close")) \
    .withColumn("price_change_pct",
        F.round((F.col("price_change") / F.col("prev_close")) * 100, 4)
    ) \
    .withColumn("intraday_range",
        F.round(F.col("high") - F.col("low"), 2)
    ) \
    .withColumn("intraday_range_pct",
        F.round((F.col("intraday_range") / F.col("open")) * 100, 4)
    ) \
    .withColumn("ma_7", F.round(F.avg("close").over(window_ticker_7d), 2)) \
    .withColumn("ma_14", F.round(F.avg("close").over(window_ticker_14d), 2)) \
    .withColumn("ma_30", F.round(F.avg("close").over(window_ticker_30d), 2)) \
    .withColumn("vol_ma_7", F.round(F.avg("volume").over(window_ticker_7d), 0)) \
    .withColumn("volatility_7d",
        F.round(F.stddev("close").over(window_ticker_7d), 4)
    ) \
    .withColumn("above_ma_7",
        (F.col("close") > F.col("ma_7")).cast(IntegerType())
    ) \
    .withColumn("above_ma_30",
        (F.col("close") > F.col("ma_30")).cast(IntegerType())
    ) \
    .withColumn("volume_spike",
        (F.col("volume") > F.col("vol_ma_7") * 2).cast(IntegerType())
    )


# ── Step 5: Risk Metrics (Actuarial value-add!) ───────────────────────────────
logger.info("Step 5: Calculating risk metrics...")

clean_df = clean_df \
    .withColumn("value_at_risk_1d",
        F.round(F.col("close") * F.col("volatility_7d") * 1.645, 2)
    ) \
    .withColumn("risk_tier",
        F.when(F.col("volatility_7d") < 1.0, "LOW")
         .when(F.col("volatility_7d") < 3.0, "MODERATE")
         .when(F.col("volatility_7d") < 5.0, "ELEVATED")
         .otherwise("HIGH")
    )


# ── Step 6: Select Final Columns ──────────────────────────────────────────────
logger.info("Step 6: Selecting final columns...")

final_df = clean_df.select(
    "ticker", "timestamp", "date", "year", "month",
    "week_of_year", "day_of_week", "hour",
    "open", "high", "low", "close", "volume",
    "price_change", "price_change_pct",
    "intraday_range", "intraday_range_pct",
    "ma_7", "ma_14", "ma_30",
    "vol_ma_7", "volatility_7d",
    "above_ma_7", "above_ma_30", "volume_spike",
    "value_at_risk_1d", "risk_tier",
    "market_cap", "pe_ratio", "sector", "industry",
    "fifty_two_week_high", "fifty_two_week_low",
    "source", "pipeline_version"
)


# ── Step 7: Write Parquet to S3 Processed Zone ───────────────────────────────
logger.info("Step 7: Writing Parquet to S3 processed zone...")

output_path = f"s3://{args['DEST_BUCKET']}/{args['DEST_PREFIX']}"

final_df.write \
    .mode("append") \
    .partitionBy("year", "month", "ticker") \
    .parquet(output_path)

logger.info(f"✅ Written {final_df.count()} records to {output_path}")
logger.info("Glue job complete!")

job.commit()
