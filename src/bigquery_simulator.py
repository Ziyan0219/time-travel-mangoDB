"""
BigQuery Simulator (powered by DuckDB)
=======================================
Simulates BigQuery's append-only event log — the heart of time travel.

PERSISTENCE:
  db_path = ":memory:"       → data lives only for this process (default for tests)
  db_path = "stocks.duckdb"  → data persists to disk across restarts

WHY DUCKDB PERFECTLY SIMULATES BIGQUERY:
  - Both are columnar storage engines (great for analytical queries)
  - Both support standard SQL
  - DuckDB runs in-process with zero setup

THE APPEND-ONLY DESIGN:
  Every price change becomes a NEW ROW. We never UPDATE or DELETE rows.
  Result: complete audit trail, full time travel, but table grows forever.

REAL BIGQUERY OPTIMIZATIONS (simulated here in spirit):
  - Partition by event_time  → queries on a time range only scan relevant days
  - Cluster by ticker        → queries for one ticker skip irrelevant data

THE TIME TRAVEL QUERY PATTERN:
  "What was the price of AAPL at time T?"

  SELECT ticker, price, event_time
  FROM stock_events
  WHERE ticker = 'AAPL'
    AND event_time <= T          -- only events UP TO that moment
  ORDER BY event_time DESC       -- most recent first
  LIMIT 1                        -- grab the closest one
"""

import os
import json
import duckdb
import time
import threading
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any

# Minimum seconds between snapshot exports.
# At 100 events/sec this means ~1 export every 0.5s instead of 100 exports/s.
_SNAPSHOT_THROTTLE_SECS = 0.5


class BigQuerySimulator:
    """
    An append-only event log backed by DuckDB.
    Every insert adds a row. No UPDATEs. No DELETEs.
    """

    def __init__(self, db_path: str = ":memory:", read_only: bool = False,
                 snapshot_path: str = None):
        self.db_path          = db_path
        self.read_only        = read_only
        self.snapshot_path    = snapshot_path   # Parquet export path for cross-process reads
        self._last_snapshot_t = 0.0             # monotonic time of last export (throttle)
        self._last_insert_t   = 0.0             # monotonic time of last insert (for flusher)
        self.conn = duckdb.connect(db_path, read_only=read_only)
        if not read_only:
            self._create_table()
        if snapshot_path and not read_only:
            self._start_snapshot_flusher()

    @classmethod
    def from_snapshot(cls, snapshot_path: str) -> "BigQuerySimulator":
        """
        Load an in-memory DuckDB from the JSON snapshot written by
        _export_snapshot(). Used by query.py when connect.py holds
        the .duckdb file open (DuckDB exclusive lock on Windows).

        WHY JSON INSTEAD OF PARQUET:
          DuckDB's COPY TO (FORMAT PARQUET) tries to overwrite an existing
          file via its own I/O layer. On Windows this fails silently when the
          destination already exists and is momentarily held by the OS cache.
          Writing via Python's built-in json + os.replace() is atomic, uses
          standard OS file APIs, and works on every platform.
        """
        with open(snapshot_path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        instance = cls.__new__(cls)
        instance.db_path          = ":memory:"
        instance.read_only        = False
        instance.snapshot_path    = snapshot_path
        instance._last_snapshot_t = 0.0
        instance._last_insert_t   = 0.0
        instance.conn             = duckdb.connect(":memory:")
        instance._create_table()
        for r in rows:
            instance.conn.execute(
                "INSERT INTO stock_events VALUES (?, ?, ?, ?, ?, ?)",
                [
                    r["ticker"],
                    r["price"],
                    r["volume"],
                    datetime.fromisoformat(r["event_time"]),
                    r["operation_type"],
                    datetime.fromisoformat(r["ingestion_time"]),
                ],
            )
        return instance

    def _start_snapshot_flusher(self):
        """
        Background thread that catches inserts missed by the inline throttle.

        If the last insert in a burst was skipped by _export_snapshot()'s
        throttle, no further event will trigger a re-export. This thread
        wakes every (throttle + 0.1)s and forces an export whenever
        _last_insert_t > _last_snapshot_t.
        """
        def _flusher():
            while True:
                time.sleep(_SNAPSHOT_THROTTLE_SECS + 0.1)
                if self._last_insert_t > self._last_snapshot_t:
                    self._write_snapshot()

        t = threading.Thread(target=_flusher, daemon=True)
        t.start()

    def _create_table(self):
        """
        Schema design:
          - No PRIMARY KEY (append-only means duplicates are intentional)
          - event_time is NOT UNIQUE (same ticker can have many rows)
          - In real BigQuery: PARTITION BY DATE(event_time) CLUSTER BY ticker
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_events (
                ticker          VARCHAR   NOT NULL,
                price           DOUBLE    NOT NULL,
                volume          BIGINT    NOT NULL,
                event_time      TIMESTAMP NOT NULL,
                operation_type  VARCHAR   NOT NULL,
                ingestion_time  TIMESTAMP NOT NULL
            )
        """)

    def insert(self, record: Dict[str, Any]):
        """
        Append a new row. The ONLY write operation allowed.
        No UPDATE. No DELETE. Ever.

        After each insert, if a snapshot_path is configured, we export the full
        table to a Parquet file. This lets query.py read from the Parquet
        snapshot while connect.py holds the DuckDB file open — working around
        DuckDB's single-writer exclusive file lock.
        """
        self.conn.execute(
            "INSERT INTO stock_events VALUES (?, ?, ?, ?, ?, ?)",
            [
                record["ticker"],
                record["price"],
                record["volume"],
                record["event_time"],
                record["operation_type"],
                record["ingestion_time"],
            ],
        )
        self._last_insert_t = time.monotonic()   # flusher watches this
        if self.snapshot_path:
            self._export_snapshot()

    def _export_snapshot(self):
        """Throttle gate — skip if called too soon, otherwise delegate."""
        now = time.monotonic()
        if now - self._last_snapshot_t < _SNAPSHOT_THROTTLE_SECS:
            return
        self._write_snapshot()

    def _write_snapshot(self):
        """
        Serialize stock_events to a JSON file using Python's json module.

        WHY NOT DuckDB COPY TO PARQUET:
          DuckDB's COPY TO rewrites a file through its own I/O layer.
          On Windows, overwriting an existing file this way can fail silently
          (the OS may have the file cached or briefly locked). The failure is
          swallowed, the snapshot stays at the old version, and query.py
          never sees new data.

        THIS APPROACH:
          1. Fetch all rows from DuckDB into Python memory (fast for ≤1M rows)
          2. Serialise timestamps as ISO-8601 strings (lossless, portable)
          3. Write to a .tmp file first, then os.replace() — which on Windows
             uses MoveFileEx(MOVEFILE_REPLACE_EXISTING) for an atomic swap,
             so readers never see a half-written file
        """
        try:
            rows = self.conn.execute("""
                SELECT ticker, price, volume,
                       CAST(event_time     AS VARCHAR) AS event_time,
                       operation_type,
                       CAST(ingestion_time AS VARCHAR) AS ingestion_time
                FROM stock_events
            """).fetchall()
            data = [
                {
                    "ticker":         r[0],
                    "price":          r[1],
                    "volume":         r[2],
                    "event_time":     r[3],
                    "operation_type": r[4],
                    "ingestion_time": r[5],
                }
                for r in rows
            ]
            tmp = self.snapshot_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, self.snapshot_path)   # atomic on Windows + POSIX
            self._last_snapshot_t = time.monotonic()
        except Exception:
            pass   # snapshot failure must never crash the main pipeline

    def is_exact_duplicate(self, ticker: str, price: float, event_time: datetime) -> bool:
        """
        Check if this exact (ticker, price, event_time) combo already exists.
        Used for idempotent inserts — if the same event arrives twice (Pub/Sub
        at-least-once delivery), we skip re-inserting.
        """
        count = self.conn.execute("""
            SELECT COUNT(*) FROM stock_events
            WHERE ticker = ? AND price = ? AND event_time = ?
        """, [ticker, price, event_time]).fetchone()[0]
        return count > 0

    # ─────────────────────────────────────────────────────────────────────
    # TIME TRAVEL QUERIES
    # ─────────────────────────────────────────────────────────────────────

    def time_travel_query(self, ticker: str, as_of_time: datetime) -> Optional[Tuple]:
        """
        ⭐ THE CORE TIME TRAVEL QUERY ⭐

        "What was {ticker}'s price at {as_of_time}?"

        Returns (ticker, price, volume, event_time) or None if no data exists
        for that ticker at or before as_of_time.
        """
        return self.conn.execute("""
            SELECT ticker, price, volume, event_time
            FROM stock_events
            WHERE ticker = ?
              AND event_time <= ?
            ORDER BY event_time DESC
            LIMIT 1
        """, [ticker, as_of_time]).fetchone()

    def get_all_tickers_snapshot(self, as_of_time: datetime) -> List[Tuple]:
        """
        Point-in-time snapshot: latest price for ALL tickers as of a given time.

        Uses DISTINCT ON (DuckDB) — equivalent to BigQuery's:
          QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY event_time DESC) = 1
        """
        return self.conn.execute("""
            SELECT DISTINCT ON (ticker)
                ticker, price, volume, event_time
            FROM stock_events
            WHERE event_time <= ?
            ORDER BY ticker, event_time DESC
        """, [as_of_time]).fetchall()

    def get_price_history(self, ticker: str) -> List[Tuple]:
        """Full chronological price history for a ticker."""
        return self.conn.execute("""
            SELECT ticker, price, volume, event_time
            FROM stock_events
            WHERE ticker = ?
            ORDER BY event_time ASC
        """, [ticker]).fetchall()

    def get_price_between(self, ticker: str, start: datetime, end: datetime) -> List[Tuple]:
        """Price changes within a time window."""
        return self.conn.execute("""
            SELECT ticker, price, volume, event_time
            FROM stock_events
            WHERE ticker = ?
              AND event_time BETWEEN ? AND ?
            ORDER BY event_time ASC
        """, [ticker, start, end]).fetchall()

    def get_known_tickers(self) -> List[str]:
        """Return all tickers that have at least one event."""
        rows = self.conn.execute(
            "SELECT DISTINCT ticker FROM stock_events ORDER BY ticker"
        ).fetchall()
        return [r[0] for r in rows]

    def get_latest_event_time(self, ticker: str) -> Optional[datetime]:
        """Return the most recent event_time for a ticker, or None."""
        row = self.conn.execute("""
            SELECT MAX(event_time) FROM stock_events WHERE ticker = ?
        """, [ticker]).fetchone()
        return row[0] if row and row[0] else None

    # ─────────────────────────────────────────────────────────────────────
    # STATS
    # ─────────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        row_count    = self.conn.execute("SELECT COUNT(*) FROM stock_events").fetchone()[0]
        ticker_count = self.conn.execute("SELECT COUNT(DISTINCT ticker) FROM stock_events").fetchone()[0]
        return {"total_rows": row_count, "unique_tickers": ticker_count}

    def close(self):
        """Explicitly release the DuckDB connection and its file lock."""
        try:
            self.conn.close()
        except Exception:
            pass

    def get_latest_state(self) -> List[Tuple]:
        """
        Return the most recent price for every known ticker.
        Used to reconstruct MongoDB's in-memory state on pipeline restart.
        """
        return self.conn.execute("""
            SELECT DISTINCT ON (ticker)
                ticker, price, volume, event_time
            FROM stock_events
            ORDER BY ticker, event_time DESC
        """).fetchall()
