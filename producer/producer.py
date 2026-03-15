"""
Financial Data Producer
Pulls live stock data from Yahoo Finance and streams to Kinesis
Run this script to start streaming data into your pipeline
"""

import json
import time
import boto3
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# Default tickers to track
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "JPM", "BAC", "GS",        # Banks
    "UNH", "CVS",               # Healthcare
    "XOM", "CVX"                # Energy
]


class FinancialDataProducer:
    """
    Pulls stock data from Yahoo Finance and streams
    records to AWS Kinesis Data Streams
    """

    def __init__(self, stream_name: str, region: str = "us-east-1", profile: str = None):
        self.stream_name = stream_name
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.kinesis = session.client("kinesis", region_name=region)
        self.records_sent = 0
        self.errors = 0

    def fetch_stock_data(self, ticker: str) -> dict:
        """Fetch latest stock data for a single ticker."""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m")

            if hist.empty:
                logger.warning(f"No data returned for {ticker}")
                return None

            latest = hist.iloc[-1]
            info = stock.info

            record = {
                "ticker": ticker,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "open": round(float(latest["Open"]), 2),
                "high": round(float(latest["High"]), 2),
                "low": round(float(latest["Low"]), 2),
                "close": round(float(latest["Close"]), 2),
                "volume": int(latest["Volume"]),
                "market_cap": info.get("marketCap", 0),
                "pe_ratio": info.get("trailingPE", 0),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh", 0),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow", 0),
                "moving_avg_50": info.get("fiftyDayAverage", 0),
                "moving_avg_200": info.get("twoHundredDayAverage", 0),
                "source": "yahoo_finance",
                "pipeline_version": "1.0.0"
            }

            # Calculate simple indicators
            if record["moving_avg_50"] and record["close"]:
                record["above_50ma"] = record["close"] > record["moving_avg_50"]

            if record["fifty_two_week_high"] and record["fifty_two_week_low"]:
                range_52w = record["fifty_two_week_high"] - record["fifty_two_week_low"]
                if range_52w > 0:
                    record["position_in_52w_range"] = round(
                        (record["close"] - record["fifty_two_week_low"]) / range_52w * 100, 2
                    )

            return record

        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return None

    def send_to_kinesis(self, record: dict) -> bool:
        """Send a single record to Kinesis Data Stream."""
        try:
            self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(record).encode("utf-8"),
                PartitionKey=record["ticker"]  # Partition by ticker symbol
            )
            self.records_sent += 1
            logger.info(f"✅ Sent: {record['ticker']} @ ${record['close']} | "
                       f"Vol: {record['volume']:,}")
            return True

        except Exception as e:
            logger.error(f"Failed to send {record['ticker']} to Kinesis: {e}")
            self.errors += 1
            return False

    def send_batch_to_kinesis(self, records: list) -> int:
        """Send multiple records in a batch (more efficient)."""
        if not records:
            return 0

        kinesis_records = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": r["ticker"]
            }
            for r in records
        ]

        try:
            response = self.kinesis.put_records(
                StreamName=self.stream_name,
                Records=kinesis_records
            )
            failed = response.get("FailedRecordCount", 0)
            sent = len(records) - failed
            self.records_sent += sent
            self.errors += failed

            if failed > 0:
                logger.warning(f"⚠️ {failed} records failed in batch")

            return sent

        except Exception as e:
            logger.error(f"Batch send failed: {e}")
            return 0

    def run(self, tickers: list, interval_seconds: int = 60, max_iterations: int = None):
        """
        Main loop — fetch and stream data continuously.

        Args:
            tickers: List of stock ticker symbols
            interval_seconds: How often to fetch data (default 60s)
            max_iterations: Stop after N iterations (None = run forever)
        """
        logger.info(f"🚀 Starting producer for {len(tickers)} tickers")
        logger.info(f"   Stream: {self.stream_name}")
        logger.info(f"   Interval: {interval_seconds}s")
        logger.info(f"   Tickers: {', '.join(tickers)}")

        iteration = 0

        try:
            while True:
                iteration += 1
                logger.info(f"\n--- Iteration {iteration} | "
                           f"{datetime.now().strftime('%H:%M:%S')} ---")

                records = []
                for ticker in tickers:
                    record = self.fetch_stock_data(ticker)
                    if record:
                        records.append(record)
                    time.sleep(0.5)  # Avoid Yahoo Finance rate limiting

                # Send batch to Kinesis
                sent = self.send_batch_to_kinesis(records)
                logger.info(f"Batch complete: {sent}/{len(tickers)} sent | "
                           f"Total: {self.records_sent} | Errors: {self.errors}")

                # Check if we should stop
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations}). Stopping.")
                    break

                # Wait before next iteration
                logger.info(f"⏳ Waiting {interval_seconds}s before next fetch...")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("\n⛔ Producer stopped by user")

        finally:
            logger.info(f"\n📊 Final Stats:")
            logger.info(f"   Iterations: {iteration}")
            logger.info(f"   Records Sent: {self.records_sent}")
            logger.info(f"   Errors: {self.errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Data Kinesis Producer")
    parser.add_argument("--stream", type=str, default="financial-data-stream",
                       help="Kinesis stream name")
    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS,
                       help="Stock ticker symbols")
    parser.add_argument("--interval", type=int, default=60,
                       help="Fetch interval in seconds")
    parser.add_argument("--iterations", type=int, default=None,
                       help="Max iterations (default: run forever)")
    parser.add_argument("--region", type=str, default="us-east-1",
                       help="AWS region")
    parser.add_argument("--profile", type=str, default=None,
                       help="AWS profile name")
    args = parser.parse_args()

    producer = FinancialDataProducer(
        stream_name=args.stream,
        region=args.region,
        profile=args.profile
    )

    producer.run(
        tickers=args.tickers,
        interval_seconds=args.interval,
        max_iterations=args.iterations
    )
