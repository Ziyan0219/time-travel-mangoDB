"""
Stock Price Time Travel Pipeline
==================================
Orchestrates all four components and adds:
  - Persistence     : data survives process restarts via DuckDB file or BigQuery
  - Verbose mode    : see every byte flowing through each stage
  - Edge cases      : validation, duplicates, out-of-order, unknown tickers
  - Observability   : processing lag, late event detection, data freshness

BACKENDS:
  use_bigquery=False (default)  →  DuckDB local file (bigquery_simulator.py)
  use_bigquery=True             →  Real Google Cloud BigQuery (bigquery_client.py)

  Or set USE_BIGQUERY=true in .env to switch without changing code.
"""

import os
from datetime import datetime
from typing import Optional, List, Tuple

from src.mongo_simulator import MongoCollection, ValidationError
from src.pubsub_simulator import PubSubTopic
from src.cloud_function import transform_change_event
from src.bigquery_simulator import BigQuerySimulator
from src.metrics import PipelineMetrics

# Real BigQuery client — imported lazily so DuckDB-only installs still work
_bigquery_client_cls = None

def _get_bigquery_client_cls():
    global _bigquery_client_cls
    if _bigquery_client_cls is None:
        from src.bigquery_client import BigQueryClient
        _bigquery_client_cls = BigQueryClient
    return _bigquery_client_cls

# ── ANSI colors ────────────────────────────────────────────────────────────────
_R    = "\033[0m"
_G    = "\033[92m"
_Y    = "\033[93m"
_C    = "\033[96m"
_D    = "\033[2m"
_BOLD = "\033[1m"

DEFAULT_DB       = "/tmp/stocks.duckdb"
DEFAULT_SNAPSHOT = "/tmp/stocks_snapshot.json"


class TimeTravelResult:
    """
    Wraps a time-travel query result with a status code so callers
    can distinguish between "no data yet" and "no data at that time".
    """
    OK      = "ok"
    NO_DATA = "no_data"   # ticker exists but no event before as_of_time
    UNKNOWN = "unknown"   # ticker never seen at all

    def __init__(self, status, ticker, as_of_time,
                 price=None, volume=None, event_time=None):
        self.status     = status
        self.ticker     = ticker
        self.as_of_time = as_of_time
        self.price      = price
        self.volume     = volume
        self.event_time = event_time

    @property
    def found(self):
        return self.status == self.OK

    def __repr__(self):
        if self.found:
            return (f"TimeTravelResult(ticker={self.ticker}, price={self.price}, "
                    f"event_time={self.event_time})")
        return f"TimeTravelResult(status={self.status}, ticker={self.ticker})"


class StockPipeline:
    """
    Full pipeline: MongoDB → Change Streams → Pub/Sub → Cloud Function → BigQuery.

    Parameters
    ----------
    verbose      : bool — Print a detailed trace for every price change event.
    persist      : bool — Use a DuckDB file so data survives restarts (DuckDB mode only).
    db_path      : str  — Override the DuckDB file path (when persist=True).
    use_bigquery : bool — Use real Google Cloud BigQuery instead of DuckDB.
                          Defaults to the USE_BIGQUERY env var, or False.
    """

    def __init__(self, verbose=False, persist=False, db_path=DEFAULT_DB,
                 read_only=False, snapshot_path=DEFAULT_SNAPSHOT,
                 use_bigquery=None):
        self.verbose       = verbose
        self.persist       = persist
        self.read_only     = read_only

        # ── Decide backend ────────────────────────────────────────────────
        if use_bigquery is None:
            use_bigquery = os.getenv("USE_BIGQUERY", "").lower() in ("1", "true", "yes")
        self.use_bigquery = use_bigquery

        # ── Components ────────────────────────────────────────────────────
        self.mongo   = MongoCollection("stocks")
        self.topic   = PubSubTopic("stock-price-changes")
        self.sub     = self.topic.subscribe("bigquery-writer")
        self.metrics = PipelineMetrics()

        self._events_processed   = 0
        self._skipped_duplicates = 0

        if self.use_bigquery:
            # ── Real BigQuery backend ──────────────────────────────────────
            BigQueryClient = _get_bigquery_client_cls()
            self.bigquery  = BigQueryClient()
            self.snapshot_path = None
            is_restart = True   # BigQuery always has persistent state

            if verbose:
                print(f"{_G}[Pipeline]{_R} Backend: Google Cloud BigQuery  "
                      f"({self.bigquery.table_id})")
        else:
            # ── DuckDB backend (original behavior) ────────────────────────
            self.snapshot_path = snapshot_path if persist else None
            resolved_db  = db_path if persist else ":memory:"
            is_restart   = persist and os.path.exists(resolved_db)

            self.bigquery = BigQuerySimulator(
                db_path=resolved_db,
                read_only=read_only,
                snapshot_path=self.snapshot_path if not read_only else None,
            )
            if verbose:
                mode = f"persistent ({resolved_db})" if persist else "in-memory"
                print(f"{_G}[Pipeline]{_R} Backend: DuckDB — {mode}")

        # ── Restart: restore MongoDB state from persistent storage ───────────
        if is_restart:
            self._restore_mongo_from_bigquery()
            if verbose:
                n = len(self.mongo.find_all())
                backend = "BigQuery" if self.use_bigquery else "DuckDB"
                print(f"{_G}[Pipeline]{_R} Restarted — "
                      f"restored {n} tickers from {backend}")

    def _restore_mongo_from_bigquery(self):
        for ticker, price, volume, event_time in self.bigquery.get_latest_state():
            self.mongo.restore(ticker, price, volume, event_time)

    # ─────────────────────────────────────────────────────────────────────
    # VERBOSE HELPERS
    # ─────────────────────────────────────────────────────────────────────

    def _vprint(self, stage, msg, color=_D):
        if self.verbose:
            print(f"  {color}[{stage:14s}]{_R}  {msg}")

    def _vsep(self, char="─", width=62):
        if self.verbose:
            print(f"  {_D}{char * width}{_R}")

    def _vheader(self, ticker, price, ts, old_price):
        if self.verbose:
            change = ""
            if old_price is not None:
                arrow  = "↑" if price > old_price else "↓" if price < old_price else "="
                change = f"  {arrow} {price - old_price:+.2f}"
            print()
            self._vsep()
            print(f"  {_BOLD}{ticker}{_R}  ${price:.2f}{change}   "
                  f"{_D}@ {ts.strftime('%a %Y-%m-%d %H:%M:%S')}{_R}")
            self._vsep()

    # ─────────────────────────────────────────────────────────────────────
    # WRITE PATH
    # ─────────────────────────────────────────────────────────────────────

    def record_price_change(self, ticker, price, volume, timestamp,
                            _quiet=False) -> bool:
        """
        Route a price change through all five pipeline stages.
        Raises ValidationError on bad input.
        Returns True if processed, True if skipped duplicate (check stats).
        """
        quiet = _quiet or not self.verbose

        # ① MongoDB in-place update (validation happens here)
        old_price = self.mongo.upsert(ticker, price, volume, timestamp)
        if not quiet:
            self._vheader(ticker, price, timestamp, old_price)
            if old_price is not None:
                self._vprint("① MongoDB",
                             f"in-place update  ${old_price:.2f} → ${price:.2f}  "
                             f"{_Y}(old value OVERWRITTEN and LOST){_R}")
            else:
                self._vprint("① MongoDB", f"new document inserted for {ticker}")

        # ② Change Stream → Pub/Sub
        for event in self.mongo.drain_change_stream():
            if not quiet:
                self._vprint("② Change Stream",
                             f"captured '{event.operation_type}' event  "
                             f"{_D}cluster_time={event.cluster_time.strftime('%H:%M:%S')}{_R}")

            msg_id = self.topic.publish(event)
            if not quiet:
                self._vprint("③ Pub/Sub",
                             f"published {_C}{msg_id}{_R}  "
                             f"{_D}(pending in sub: {self.sub.pending_count}){_R}")

        # ③④ Cloud Function + BigQuery
        for msg in self.sub.pull():
            bq_record = transform_change_event(msg.data)

            if not quiet:
                self._vprint("④ Cloud Fn",
                             f"transform complete  "
                             f"event_time={bq_record['event_time'].strftime('%H:%M:%S')}  "
                             f"ingestion_time={bq_record['ingestion_time'].strftime('%H:%M:%S.%f')[:12]}")

            is_dup = self.bigquery.is_exact_duplicate(
                bq_record["ticker"], bq_record["price"], bq_record["event_time"])

            # ── Record metric BEFORE deciding to skip ─────────────────────
            self.metrics.record(
                ticker=bq_record["ticker"],
                event_time=bq_record["event_time"],
                publish_time=msg.publish_time,
                ingestion_time=bq_record["ingestion_time"],
                is_duplicate=is_dup,
            )

            if is_dup:
                self._skipped_duplicates += 1
                if not quiet:
                    self._vprint("⑤ BigQuery",
                                 f"{_Y}SKIPPED duplicate "
                                 f"(ticker={bq_record['ticker']}, "
                                 f"event_time={bq_record['event_time']}){_R}")
                continue

            self.bigquery.insert(bq_record)
            self._events_processed += 1
            if not quiet:
                self._vprint("⑤ BigQuery",
                             f"{_G}INSERT{_R} row #{self._events_processed}  "
                             f"{_D}(total: {self.bigquery.get_stats()['total_rows']}){_R}")

        return True

    # ─────────────────────────────────────────────────────────────────────
    # READ PATH
    # ─────────────────────────────────────────────────────────────────────

    def time_travel(self, ticker, as_of_time) -> TimeTravelResult:
        if ticker not in self.bigquery.get_known_tickers():
            return TimeTravelResult(TimeTravelResult.UNKNOWN, ticker, as_of_time)

        row = self.bigquery.time_travel_query(ticker, as_of_time)
        if row is None:
            return TimeTravelResult(TimeTravelResult.NO_DATA, ticker, as_of_time)

        return TimeTravelResult(TimeTravelResult.OK, ticker, as_of_time,
                                price=row[1], volume=row[2], event_time=row[3])

    def snapshot_all(self, as_of_time) -> List[Tuple]:
        return self.bigquery.get_all_tickers_snapshot(as_of_time)

    def current_price_from_mongo(self, ticker):
        return self.mongo.find_one(ticker)

    # ─────────────────────────────────────────────────────────────────────
    # STATS + OBSERVABILITY
    # ─────────────────────────────────────────────────────────────────────

    @property
    def stats(self):
        bq = self.bigquery.get_stats()
        return {
            **bq,
            "events_processed":   self._events_processed,
            "duplicates_skipped": self._skipped_duplicates,
        }

    def health_report(self) -> dict:
        """Full observability report: lag, late events, freshness, counts."""
        return self.metrics.full_report()
