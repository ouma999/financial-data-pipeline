"""
Test Suite — Financial Data Pipeline
Tests producer logic without requiring live AWS or Yahoo Finance
Run: pytest tests/ -v
"""

import json
import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../producer"))


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def mock_kinesis():
    with patch("boto3.Session") as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        mock_client.put_record.return_value = {"ShardId": "shardId-000", "SequenceNumber": "123"}
        mock_client.put_records.return_value = {"FailedRecordCount": 0, "Records": []}
        yield mock_client


@pytest.fixture
def sample_record():
    return {
        "ticker": "AAPL",
        "timestamp": "2026-03-14T14:00:00+00:00",
        "open": 192.50,
        "high": 194.20,
        "low": 191.80,
        "close": 193.40,
        "volume": 52000000,
        "market_cap": 3000000000000,
        "pe_ratio": 28.5,
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "fifty_two_week_high": 220.0,
        "fifty_two_week_low": 164.0,
        "moving_avg_50": 188.0,
        "moving_avg_200": 182.0,
        "source": "yahoo_finance",
        "pipeline_version": "1.0.0"
    }


@pytest.fixture
def producer(mock_kinesis):
    from producer import FinancialDataProducer
    return FinancialDataProducer(
        stream_name="test-stream",
        region="us-east-1"
    )


# ─────────────────────────────────────────────
# Unit Tests — Record Structure
# ─────────────────────────────────────────────

class TestRecordStructure:

    def test_required_fields_present(self, sample_record):
        required = ["ticker", "timestamp", "open", "high", "low", "close", "volume"]
        for field in required:
            assert field in sample_record, f"Missing required field: {field}"

    def test_price_values_positive(self, sample_record):
        assert sample_record["open"] > 0
        assert sample_record["high"] > 0
        assert sample_record["low"] > 0
        assert sample_record["close"] > 0

    def test_high_greater_than_low(self, sample_record):
        assert sample_record["high"] >= sample_record["low"]

    def test_volume_positive(self, sample_record):
        assert sample_record["volume"] > 0

    def test_ticker_uppercase(self, sample_record):
        assert sample_record["ticker"] == sample_record["ticker"].upper()

    def test_source_field_present(self, sample_record):
        assert sample_record["source"] == "yahoo_finance"

    def test_pipeline_version_present(self, sample_record):
        assert "pipeline_version" in sample_record


# ─────────────────────────────────────────────
# Unit Tests — Producer Logic
# ─────────────────────────────────────────────

class TestProducerLogic:

    def test_send_to_kinesis_success(self, producer, sample_record):
        result = producer.send_to_kinesis(sample_record)
        assert result is True
        assert producer.records_sent == 1
        assert producer.errors == 0

    def test_send_to_kinesis_increments_counter(self, producer, sample_record):
        producer.send_to_kinesis(sample_record)
        producer.send_to_kinesis(sample_record)
        assert producer.records_sent == 2

    def test_send_batch_success(self, producer, sample_record):
        records = [sample_record] * 5
        sent = producer.send_batch_to_kinesis(records)
        assert sent == 5

    def test_send_empty_batch(self, producer):
        sent = producer.send_batch_to_kinesis([])
        assert sent == 0

    def test_kinesis_failure_increments_error(self, producer, sample_record):
        producer.kinesis.put_record.side_effect = Exception("Kinesis unavailable")
        result = producer.send_to_kinesis(sample_record)
        assert result is False
        assert producer.errors == 1

    def test_partition_key_is_ticker(self, producer, sample_record):
        producer.send_to_kinesis(sample_record)
        call_args = producer.kinesis.put_record.call_args
        assert call_args[1]["PartitionKey"] == "AAPL"

    def test_data_is_json_serializable(self, producer, sample_record):
        producer.send_to_kinesis(sample_record)
        call_args = producer.kinesis.put_record.call_args
        data = call_args[1]["Data"]
        parsed = json.loads(data.decode("utf-8"))
        assert parsed["ticker"] == "AAPL"


# ─────────────────────────────────────────────
# Unit Tests — Technical Indicators
# ─────────────────────────────────────────────

class TestTechnicalIndicators:

    def test_above_50ma_calculation(self, sample_record):
        close = sample_record["close"]
        ma_50 = sample_record["moving_avg_50"]
        expected = close > ma_50
        assert expected is True  # 193.40 > 188.0

    def test_52w_range_position(self, sample_record):
        close = sample_record["close"]
        high_52w = sample_record["fifty_two_week_high"]
        low_52w = sample_record["fifty_two_week_low"]
        range_52w = high_52w - low_52w
        position = (close - low_52w) / range_52w * 100
        assert 0 <= position <= 100

    def test_intraday_range(self, sample_record):
        intraday_range = sample_record["high"] - sample_record["low"]
        assert intraday_range >= 0
        assert intraday_range == pytest.approx(2.40, abs=0.01)


# ─────────────────────────────────────────────
# Integration Tests — Data Flow
# ─────────────────────────────────────────────

class TestDataFlow:

    @patch("yfinance.Ticker")
    def test_fetch_stock_data_structure(self, mock_yf, producer):
        mock_ticker = MagicMock()
        mock_yf.return_value = mock_ticker

        mock_hist = pd.DataFrame({
            "Open": [192.50],
            "High": [194.20],
            "Low": [191.80],
            "Close": [193.40],
            "Volume": [52000000]
        })
        mock_ticker.history.return_value = mock_hist
        mock_ticker.info = {
            "marketCap": 3000000000000,
            "trailingPE": 28.5,
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "fiftyTwoWeekHigh": 220.0,
            "fiftyTwoWeekLow": 164.0,
            "fiftyDayAverage": 188.0,
            "twoHundredDayAverage": 182.0
        }

        record = producer.fetch_stock_data("AAPL")

        assert record is not None
        assert record["ticker"] == "AAPL"
        assert record["close"] == 193.40
        assert record["volume"] == 52000000
        assert record["sector"] == "Technology"

    @patch("yfinance.Ticker")
    def test_fetch_empty_data_returns_none(self, mock_yf, producer):
        mock_ticker = MagicMock()
        mock_yf.return_value = mock_ticker
        mock_ticker.history.return_value = pd.DataFrame()

        record = producer.fetch_stock_data("INVALID")
        assert record is None
