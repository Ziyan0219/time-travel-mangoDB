"""
Tests: Core Time Travel Query Logic
=====================================
Every test here answers one question: "does the time travel query
return the right price for a given (ticker, timestamp) pair?"

We test six distinct scenarios:
  1. Exact timestamp match
  2. Query falls between two updates (no event at that exact minute)
  3. Query is after all known events (should return the last known price)
  4. Unknown ticker
  5. Query before any data exists for this ticker
  6. Multi-ticker snapshot at a point in time
"""

import pytest
from datetime import datetime
from pipeline import StockPipeline, TimeTravelResult


# ── Helpers ───────────────────────────────────────────────────────────────────

def fresh():
    return StockPipeline(verbose=False)

def ts(month, day, hour, minute=0):
    return datetime(2026, month, day, hour, minute)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_exact_timestamp_match():
    """Query at exactly the moment a price was recorded."""
    p = fresh()
    p.record_price_change("AAPL", 175.10, 1_300_000, ts(3, 20, 9, 36))

    r = p.time_travel("AAPL", ts(3, 20, 9, 36))

    assert r.found
    assert r.price == 175.10
    assert r.event_time == ts(3, 20, 9, 36)


def test_query_between_updates_returns_most_recent():
    """
    No event at exactly 13:00 on Wednesday.
    The most recent prior event (11:30) should be returned.
    """
    p = fresh()
    p.record_price_change("AAPL", 172.40, 1_000_000, ts(3, 18,  9, 30))
    p.record_price_change("AAPL", 173.20, 1_000_000, ts(3, 18, 11, 30))
    p.record_price_change("AAPL", 176.30, 1_000_000, ts(3, 18, 14,  0))

    r = p.time_travel("AAPL", ts(3, 18, 13, 0))   # between 11:30 and 14:00

    assert r.found
    assert r.price == 173.20
    assert r.event_time == ts(3, 18, 11, 30)


def test_query_after_last_event_returns_last_known_price():
    """After the final event of the day, price stays at that value."""
    p = fresh()
    p.record_price_change("AAPL", 175.90, 1_100_000, ts(3, 20, 15, 30))

    r = p.time_travel("AAPL", ts(3, 20, 23, 59))   # end of day

    assert r.found
    assert r.price == 175.90


def test_unknown_ticker_returns_unknown_status():
    """A ticker that was never seen must not silently return wrong data."""
    p = fresh()
    p.record_price_change("AAPL", 175.10, 1_000_000, ts(3, 20, 9, 36))

    r = p.time_travel("NVDA", ts(3, 20, 9, 36))

    assert not r.found
    assert r.status == TimeTravelResult.UNKNOWN
    assert r.price is None


def test_query_before_first_event_returns_no_data():
    """
    AAPL exists in the system but its first event is Monday 9:30.
    Querying Sunday must return NO_DATA (not UNKNOWN — the ticker is known,
    just no data exists that early).
    """
    p = fresh()
    p.record_price_change("AAPL", 170.50, 1_200_000, ts(3, 16, 9, 30))  # Monday

    r = p.time_travel("AAPL", ts(3, 15, 20, 0))    # Sunday before Monday

    assert not r.found
    assert r.status == TimeTravelResult.NO_DATA


def test_snapshot_returns_correct_price_per_ticker():
    """
    Point-in-time snapshot: each ticker should return its most recent
    price AT OR BEFORE the snapshot time, not any later price.
    """
    p = fresh()
    # Monday: both tickers start
    p.record_price_change("AAPL",  170.50, 1_000_000, ts(3, 16, 9, 30))
    p.record_price_change("GOOGL", 155.20, 1_000_000, ts(3, 16, 9, 30))
    # Friday: AAPL updates (AFTER our snapshot target of Wed 12:00)
    p.record_price_change("AAPL",  175.10, 1_000_000, ts(3, 20, 9, 36))

    snapshot = p.snapshot_all(ts(3, 18, 12, 0))   # Wednesday noon
    prices = {row[0]: row[1] for row in snapshot}

    assert prices["AAPL"]  == 170.50   # Friday's 175.10 is AFTER Wed noon
    assert prices["GOOGL"] == 155.20


def test_multiple_updates_same_ticker_correct_sequence():
    """Feed 5 updates, verify time travel returns the right one at each point."""
    p = fresh()
    updates = [
        (170.50, ts(3, 16,  9, 30)),
        (171.80, ts(3, 16, 10, 15)),
        (169.90, ts(3, 16, 14,  0)),
        (172.40, ts(3, 18,  9, 30)),
        (175.10, ts(3, 20,  9, 36)),
    ]
    for price, timestamp in updates:
        p.record_price_change("AAPL", price, 1_000_000, timestamp)

    assert p.time_travel("AAPL", ts(3, 16,  9, 30)).price == 170.50
    assert p.time_travel("AAPL", ts(3, 16, 10, 15)).price == 171.80
    assert p.time_travel("AAPL", ts(3, 16, 12,  0)).price == 171.80  # between updates
    assert p.time_travel("AAPL", ts(3, 16, 14,  0)).price == 169.90
    assert p.time_travel("AAPL", ts(3, 17, 12,  0)).price == 169.90  # Tuesday, no data
    assert p.time_travel("AAPL", ts(3, 20,  9, 36)).price == 175.10
