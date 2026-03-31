"""
BigQuery Client (Real GCP)
==========================
Drop-in replacement for BigQuerySimulator, backed by Google Cloud BigQuery.

INTERFACE CONTRACT:
  Every public method matches BigQuerySimulator exactly so that pipeline.py
  needs zero changes — just swap the class being instantiated.

SETUP:
  1. pip install -r requirements.txt
  2. Create a GCP Service Account with roles:
       - BigQuery Data Editor
       - BigQuery Job User
  3. Download the JSON key and set in .env:
       GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
       GCP_PROJECT=your-project-id
       BQ_DATASET=stocks
       BQ_TABLE=stock_events
  4. In pipeline.py set use_bigquery=True (or via env var USE_BIGQUERY=true)

TABLE SCHEMA (auto-created if it doesn't exist):
  ticker          STRING    NOT NULL
  price           FLOAT64   NOT NULL
  volume          INT64     NOT NULL
  event_time      TIMESTAMP NOT NULL
  operation_type  STRING    NOT NULL
  ingestion_time  TIMESTAMP NOT NULL

  Partitioned by DATE(event_time), clustered by ticker —
  same optimizations as the real BigQuery design doc.
"""

import os
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# BigQuery schema — mirrors the DuckDB table in bigquery_simulator.py exactly
_SCHEMA = [
    bigquery.SchemaField("ticker",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("price",          "FLOAT64",   mode="REQUIRED"),
    bigquery.SchemaField("volume",         "INT64",     mode="REQUIRED"),
    bigquery.SchemaField("event_time",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("operation_type", "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
]


class BigQueryClient:
    """
    Append-only event log backed by real Google Cloud BigQuery.
    Public interface is identical to BigQuerySimulator.
    """

    def __init__(
        self,
        project:   str = None,
        dataset:   str = None,
        table:     str = None,
        key_path:  str = None,
    ):
        # ── Config (fall back to env vars) ────────────────────────────────────
        self.project  = project  or os.getenv("GCP_PROJECT")
        self.dataset  = dataset  or os.getenv("BQ_DATASET",  "stocks")
        self.table    = table    or os.getenv("BQ_TABLE",    "stock_events")
        key_path      = key_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if not self.project:
            raise ValueError(
                "GCP project not set. Add GCP_PROJECT=your-project-id to .env"
            )

        # ── Auth ──────────────────────────────────────────────────────────────
        # If GOOGLE_APPLICATION_CREDENTIALS is set in the environment (or passed
        # explicitly), the BigQuery client picks it up automatically.
        # We don't need to pass it manually — just validate it exists.
        if key_path and not os.path.exists(key_path):
            raise FileNotFoundError(
                f"Service account key not found: {key_path}\n"
                "Download it from GCP Console → IAM → Service Accounts → Keys"
            )

        self.client   = bigquery.Client(project=self.project)
        self.table_id = f"{self.project}.{self.dataset}.{self.table}"

        self._ensure_dataset()
        self._ensure_table()

    # ─────────────────────────────────────────────────────────────────────────
    # SETUP HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _ensure_dataset(self):
        """Create the dataset if it doesn't exist yet."""
        dataset_ref = f"{self.project}.{self.dataset}"
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "US"
            self.client.create_dataset(ds)
            print(f"  [BigQuery] Created dataset: {dataset_ref}")

    def _ensure_table(self):
        """
        Create the table if it doesn't exist.
        Uses DAY partitioning on event_time and clustering on ticker —
        the same optimizations described in bigquery_simulator.py.
        """
        try:
            self.client.get_table(self.table_id)
        except NotFound:
            table = bigquery.Table(self.table_id, schema=_SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_time",
            )
            table.clustering_fields = ["ticker"]
            self.client.create_table(table)
            print(f"  [BigQuery] Created table: {self.table_id}")

    # ─────────────────────────────────────────────────────────────────────────
    # WRITE
    # ─────────────────────────────────────────────────────────────────────────

    def insert(self, record: Dict[str, Any]):
        """
        Streaming insert — appends one row to BigQuery.
        No UPDATE. No DELETE. Ever.

        Uses the BigQuery Storage Write API (insert_rows_json) which is
        free for < 1 GB/day and has < 1 second latency.
        """
        row = {
            "ticker":         record["ticker"],
            "price":          float(record["price"]),
            "volume":         int(record["volume"]),
            "event_time":     record["event_time"].isoformat(),
            "operation_type": record["operation_type"],
            "ingestion_time": record["ingestion_time"].isoformat(),
        }
        errors = self.client.insert_rows_json(self.table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert error: {errors}")

    # ─────────────────────────────────────────────────────────────────────────
    # DUPLICATE CHECK
    # ─────────────────────────────────────────────────────────────────────────

    def is_exact_duplicate(
        self, ticker: str, price: float, event_time: datetime
    ) -> bool:
        """
        Idempotency guard: check if this (ticker, price, event_time) already
        exists. Protects against Pub/Sub at-least-once re-delivery.

        NOTE: BigQuery streaming buffer has a short delay (~seconds) before
        rows are visible to queries. For very high-frequency feeds you may
        see rare duplicate rows during this window — acceptable in practice.
        """
        query = """
            SELECT COUNT(*) AS cnt
            FROM `{table}`
            WHERE ticker     = @ticker
              AND price      = @price
              AND event_time = @event_time
        """.format(table=self.table_id)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker",     "STRING",    ticker),
                bigquery.ScalarQueryParameter("price",      "FLOAT64",   price),
                bigquery.ScalarQueryParameter("event_time", "TIMESTAMP", event_time.isoformat()),
            ]
        )
        result = self.client.query(query, job_config=job_config).result()
        return next(result).cnt > 0

    # ─────────────────────────────────────────────────────────────────────────
    # TIME TRAVEL QUERIES  (identical SQL to bigquery_simulator.py)
    # ─────────────────────────────────────────────────────────────────────────

    def time_travel_query(
        self, ticker: str, as_of_time: datetime
    ) -> Optional[Tuple]:
        """
        "What was {ticker}'s price at {as_of_time}?"
        Returns (ticker, price, volume, event_time) or None.
        """
        query = """
            SELECT ticker, price, volume, event_time
            FROM `{table}`
            WHERE ticker     = @ticker
              AND event_time <= @as_of_time
            ORDER BY event_time DESC
            LIMIT 1
        """.format(table=self.table_id)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker",     "STRING",    ticker),
                bigquery.ScalarQueryParameter("as_of_time", "TIMESTAMP", as_of_time.isoformat()),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        if not rows:
            return None
        r = rows[0]
        return (r.ticker, r.price, r.volume, r.event_time.replace(tzinfo=None))

    def get_all_tickers_snapshot(self, as_of_time: datetime) -> List[Tuple]:
        """Point-in-time snapshot: latest price for ALL tickers as of a given time."""
        query = """
            SELECT ticker, price, volume, event_time
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker
                        ORDER BY event_time DESC
                    ) AS rn
                FROM `{table}`
                WHERE event_time <= @as_of_time
            )
            WHERE rn = 1
            ORDER BY ticker
        """.format(table=self.table_id)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("as_of_time", "TIMESTAMP", as_of_time.isoformat()),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        return [(r.ticker, r.price, r.volume, r.event_time.replace(tzinfo=None))
                for r in rows]

    def get_price_history(self, ticker: str) -> List[Tuple]:
        """Full chronological price history for a ticker."""
        query = """
            SELECT ticker, price, volume, event_time
            FROM `{table}`
            WHERE ticker = @ticker
            ORDER BY event_time ASC
        """.format(table=self.table_id)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        return [(r.ticker, r.price, r.volume, r.event_time.replace(tzinfo=None))
                for r in rows]

    def get_price_between(
        self, ticker: str, start: datetime, end: datetime
    ) -> List[Tuple]:
        """Price changes within a time window."""
        query = """
            SELECT ticker, price, volume, event_time
            FROM `{table}`
            WHERE ticker     = @ticker
              AND event_time BETWEEN @start AND @end
            ORDER BY event_time ASC
        """.format(table=self.table_id)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING",    ticker),
                bigquery.ScalarQueryParameter("start",  "TIMESTAMP", start.isoformat()),
                bigquery.ScalarQueryParameter("end",    "TIMESTAMP", end.isoformat()),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        return [(r.ticker, r.price, r.volume, r.event_time.replace(tzinfo=None))
                for r in rows]

    def get_known_tickers(self) -> List[str]:
        """Return all tickers that have at least one event."""
        query = """
            SELECT DISTINCT ticker
            FROM `{table}`
            ORDER BY ticker
        """.format(table=self.table_id)
        rows = list(self.client.query(query).result())
        return [r.ticker for r in rows]

    def get_latest_event_time(self, ticker: str) -> Optional[datetime]:
        """Return the most recent event_time for a ticker, or None."""
        query = """
            SELECT MAX(event_time) AS latest
            FROM `{table}`
            WHERE ticker = @ticker
        """.format(table=self.table_id)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        if rows and rows[0].latest:
            return rows[0].latest.replace(tzinfo=None)
        return None

    def get_latest_state(self) -> List[Tuple]:
        """
        Most recent price for every ticker.
        Used to restore MongoDB in-memory state on pipeline restart.
        """
        query = """
            SELECT ticker, price, volume, event_time
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker ORDER BY event_time DESC
                    ) AS rn
                FROM `{table}`
            )
            WHERE rn = 1
        """.format(table=self.table_id)
        rows = list(self.client.query(query).result())
        return [(r.ticker, r.price, r.volume, r.event_time.replace(tzinfo=None))
                for r in rows]

    # ─────────────────────────────────────────────────────────────────────────
    # STATS
    # ─────────────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        query = """
            SELECT
                COUNT(*)          AS total_rows,
                COUNT(DISTINCT ticker) AS unique_tickers
            FROM `{table}`
        """.format(table=self.table_id)
        row = list(self.client.query(query).result())[0]
        return {
            "total_rows":     row.total_rows,
            "unique_tickers": row.unique_tickers,
        }

    def close(self):
        """Release the BigQuery client. (No-op in most cases — just for interface parity.)"""
        try:
            self.client.close()
        except Exception:
            pass
