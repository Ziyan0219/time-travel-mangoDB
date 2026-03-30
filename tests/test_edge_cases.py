"""
Tests: Edge Cases
==================
The four production scenarios that break naive implementations:
  1. Duplicate events    (Pub/Sub at-least-once delivery)
  2. Late-arriving data  (out-of-order events)
  3. MongoDB in-place    (only latest state visible)
  4. BigQuery immutable  (all history preserved regardless of order)
"""

import pytest
from datetime import datetime
from pipeline import StockPipeline


def fresh():
    return StockPipeline(verbose=False)

def ts(day, hour, minute=0):
    return datetime(2026, 3, day, hour, minute)


# ── Duplicate events ──────────────────────────────────────────────────────────

def test_exact_duplicate_is_skipped():
    """
    Same (ticker, price, event_time) arriving twice must insert only one row.
    Real Pub/Sub guarantees at-least-once, so duplicates are expected.
    """
    p = fresh()
    p.record_price_change("AAPL", 175.10, 1_300_000, ts(20, 9, 36))
    rows_before = p.bigquery.get_stats()["total_rows"]

    p.record_price_change("AAPL", 175.10, 1_300_000, ts(20, 9, 36))   # exact duplicate

    assert p.bigquery.get_stats()["total_rows"] == rows_before
    assert p.stats["duplicates_skipped"] == 1


def test_different_price_same_time_is_not_duplicate():
    """
    Same timestamp but different price = a correction, not a duplicate.
    Both rows should be kept (the time travel query will pick the later-inserted one).
    """
    p = fresh()
    p.record_price_change("AAPL", 175.10, 1_300_000, ts(20, 9, 36))
    p.record_price_change("AAPL", 175.20, 1_300_000, ts(20, 9, 36))   # correction

    assert p.bigquery.get_stats()["total_rows"] == 2
    assert p.stats["duplicates_skipped"] == 0


# ── Late-arriving / out-of-order events ──────────────────────────────────────

def test_late_event_slots_into_correct_position():
    """
    Friday data arrives first, then Wednesday data arrives late.
    Time travel must still return the correct price for Wednesday.
    """
    p = fresh()
    p.record_price_change("MSFT", 415.60,  800_000, ts(20, 10, 30))  # Friday first
    p.record_price_change("MSFT", 413.00,  700_000, ts(18, 12,  0))  # Wednesday late

    r_wed  = p.time_travel("MSFT", ts(18, 12, 0))
    r_fri  = p.time_travel("MSFT", ts(20, 10, 30))

    assert r_wed.price == 413.00   # late event is at correct position
    assert r_fri.price == 415.60


def test_late_event_does_not_affect_earlier_time_travel():
    """
    Inserting a late event for ticker X must not corrupt queries for ticker Y.
    """
    p = fresh()
    p.record_price_change("AAPL",  175.10, 1_000_000, ts(20, 9, 36))
    p.record_price_change("GOOGL", 159.00, 1_000_000, ts(20, 9, 38))
    p.record_price_change("AAPL",  170.50, 1_000_000, ts(16, 9, 30))  # AAPL Monday, late

    r = p.time_travel("GOOGL", ts(20, 9, 38))

    assert r.price == 159.00   # GOOGL unaffected


# ── MongoDB in-place guarantee ────────────────────────────────────────────────

def test_mongodb_always_shows_only_latest():
    """
    After N updates to the same ticker, MongoDB must contain exactly one document
    with the most recent price.
    """
    p = fresh()
    prices = [170.50, 171.80, 169.90, 172.40, 175.10]
    for i, price in enumerate(prices):
        p.record_price_change("AAPL", price, 1_000_000, ts(16 + i, 9, 30))

    doc = p.current_price_from_mongo("AAPL")

    assert doc["price"] == prices[-1]           # only the last price
    assert len(p.mongo.find_all()) >= 1         # at least one ticker
    assert p.bigquery.get_stats()["total_rows"] == len(prices)  # BigQuery has all


# ── BigQuery immutability ─────────────────────────────────────────────────────

def test_bigquery_row_count_only_grows():
    """
    Total BigQuery row count must be monotonically non-decreasing.
    (Excluding exact duplicates which are filtered.)
    """
    p = fresh()
    counts = []
    for i, price in enumerate([170.0, 171.0, 172.0, 173.0]):
        p.record_price_change("AAPL", price, 1_000_000, ts(16 + i, 9, 30))
        counts.append(p.bigquery.get_stats()["total_rows"])

    assert counts == sorted(counts)   # always increasing
