"""
MongoDB Simulator
=================
Simulates MongoDB's in-place update behavior + Change Streams.

KEY INSIGHT:
  MongoDB stores only the CURRENT state of each document.
  When AAPL's price changes from $170 → $171, the old $170 is GONE from MongoDB.
  Change Streams are MongoDB's mechanism to capture every write operation
  BEFORE the old value disappears.

VALIDATION:
  Real MongoDB allows any value. We add lightweight validation here to
  simulate what you'd typically enforce via schema validation in Atlas.
"""

import queue
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, Optional, List


@dataclass
class ChangeEvent:
    """
    A single Change Stream event emitted by MongoDB.
    Mirrors the exact structure from a real MongoDB Change Stream cursor.
    """
    operation_type: str           # "insert" or "update"
    full_document: Dict[str, Any] # Complete document AFTER the change
    updated_fields: Dict[str, Any]# Only the fields that changed
    cluster_time: datetime        # Timestamp from MongoDB's internal clock
    document_key: str             # Primary key (ticker symbol)


class ValidationError(Exception):
    """Raised when a document fails schema validation before upsert."""
    pass


class MongoCollection:
    """
    Simulates a MongoDB collection for stock prices.

    _documents  →  Dict[ticker, latest_doc]   (in-place, only current state)
    _change_stream_queue  →  Queue of ChangeEvents
    """

    def __init__(self, name: str):
        self.name = name
        self._documents: Dict[str, Dict] = {}
        self._change_stream_queue: queue.Queue = queue.Queue()

    # ─────────────────────────────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────────────────────────────

    def _validate(self, ticker: str, price: float, volume: int, timestamp: datetime):
        """
        Lightweight schema validation — similar to MongoDB Atlas's JSON Schema.
        Raises ValidationError with a descriptive message on failure.
        """
        if not isinstance(ticker, str) or not ticker.strip():
            raise ValidationError(f"ticker must be a non-empty string, got: {ticker!r}")
        if not isinstance(price, (int, float)) or price <= 0:
            raise ValidationError(f"price must be a positive number, got: {price!r}")
        if not isinstance(volume, int) or volume < 0:
            raise ValidationError(f"volume must be a non-negative integer, got: {volume!r}")
        if not isinstance(timestamp, datetime):
            raise ValidationError(f"timestamp must be a datetime object, got: {type(timestamp)}")

    # ─────────────────────────────────────────────────────────────────────
    # WRITE
    # ─────────────────────────────────────────────────────────────────────

    def upsert(self, ticker: str, price: float, volume: int, timestamp: datetime) -> Optional[float]:
        """
        Insert or update a stock document. THIS IS IN-PLACE.

        The old price is overwritten and lost from MongoDB.
        The Change Stream event fires before the overwrite so downstream
        systems can capture the new value with its timestamp.

        Returns the old price (for logging), or None if this is a new ticker.
        Raises ValidationError if inputs fail schema validation.
        """
        self._validate(ticker, price, volume, timestamp)

        is_new   = ticker not in self._documents
        old_price = self._documents[ticker]["price"] if not is_new else None

        new_doc = {
            "ticker":       ticker,
            "price":        price,
            "volume":       volume,
            "last_updated": timestamp,
        }

        # ← IN-PLACE OVERWRITE: old document is gone after this line
        self._documents[ticker] = new_doc

        event = ChangeEvent(
            operation_type="insert" if is_new else "update",
            full_document=new_doc.copy(),
            updated_fields={"price": price} if not is_new else new_doc.copy(),
            cluster_time=timestamp,
            document_key=ticker,
        )
        self._change_stream_queue.put(event)
        return old_price

    def restore(self, ticker: str, price: float, volume: int, timestamp: datetime):
        """
        Silently restore a document from persistent storage on restart.
        Does NOT emit a Change Stream event (the event was already processed
        in a previous session and is stored in BigQuery).
        """
        self._documents[ticker] = {
            "ticker":       ticker,
            "price":        price,
            "volume":       volume,
            "last_updated": timestamp,
        }

    # ─────────────────────────────────────────────────────────────────────
    # READ
    # ─────────────────────────────────────────────────────────────────────

    def find_one(self, ticker: str) -> Optional[Dict]:
        """
        Read the current state of a ticker.
        Returns None if ticker has never been seen.
        This is ALL MongoDB can tell you — no history available here.
        """
        return self._documents.get(ticker)

    def find_all(self) -> List[Dict]:
        """Return all current ticker documents."""
        return list(self._documents.values())

    # ─────────────────────────────────────────────────────────────────────
    # CHANGE STREAM
    # ─────────────────────────────────────────────────────────────────────

    def drain_change_stream(self) -> List[ChangeEvent]:
        """
        Collect all pending Change Stream events.
        In production this is a long-lived cursor. Here we drain synchronously.
        """
        events = []
        while not self._change_stream_queue.empty():
            try:
                events.append(self._change_stream_queue.get_nowait())
            except queue.Empty:
                break
        return events
