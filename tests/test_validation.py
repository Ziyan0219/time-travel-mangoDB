"""
Tests: Schema Validation
==========================
MongoDB (and our simulator) must reject bad data before it enters the pipeline.
Bad data that reaches BigQuery is permanent — it can never be deleted.
"""

import pytest
from datetime import datetime
from pipeline import StockPipeline
from src.mongo_simulator import ValidationError


TS = datetime(2026, 3, 20, 9, 36)


def fresh():
    return StockPipeline(verbose=False)


# ── Invalid inputs must be rejected ──────────────────────────────────────────

@pytest.mark.parametrize("ticker,price,volume,match", [
    ("AAPL",   -50.0, 1000, "positive"),   # negative price
    ("AAPL",     0.0, 1000, "positive"),   # zero price (stock can't be $0)
    ("AAPL",  "150",  1000, "positive"),   # price as string
    ("",       150.0, 1000, "non-empty"),  # empty ticker
    ("   ",    150.0, 1000, "non-empty"),  # whitespace-only ticker  (stripped → empty)
    ("AAPL",   150.0,   -1, "non-negative"),  # negative volume
])
def test_invalid_input_raises_validation_error(ticker, price, volume, match):
    p = fresh()
    with pytest.raises(ValidationError, match=match):
        p.record_price_change(ticker, price, volume, TS)


# ── Valid edge cases must be accepted ─────────────────────────────────────────

def test_zero_volume_is_valid():
    """Volume of 0 is unusual but valid (e.g. after-hours reference price)."""
    p = fresh()
    p.record_price_change("AAPL", 150.0, 0, TS)
    assert p.current_price_from_mongo("AAPL")["price"] == 150.0


def test_very_large_price_is_valid():
    """Berkshire Hathaway Class A trades above $600,000."""
    p = fresh()
    p.record_price_change("BRK.A", 620_000.0, 10, TS)
    assert p.current_price_from_mongo("BRK.A")["price"] == 620_000.0


def test_validation_blocks_bigquery_insert():
    """
    Critical: bad data must not reach BigQuery.
    If validation fails, BigQuery must remain empty.
    """
    p = fresh()
    with pytest.raises(ValidationError):
        p.record_price_change("AAPL", -1.0, 1000, TS)

    assert p.bigquery.get_stats()["total_rows"] == 0
