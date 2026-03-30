"""
Pipeline Observability — Metrics
==================================
Tracks the health and performance of the pipeline in real time.

THREE THINGS WE MEASURE:

1. Processing Lag (milliseconds)
   = ingestion_time − publish_time
   How long did it take from "Pub/Sub received the message"
   to "Cloud Function finished writing to BigQuery"?
   In production: spikes here mean Cloud Functions are overwhelmed.

2. Late Event Rate
   = events where event_time < the highest event_time we've seen for that ticker
   An event is "late" if it carries a timestamp older than what we've already
   processed. This happens when a MongoDB node lags behind, or the network
   delivers messages out of order.
   In production: a rising late rate means upstream replication issues.

3. Data Freshness (per ticker)
   = now − latest event_time for that ticker
   How stale is our most recent data for each stock?
   In production: if AAPL hasn't updated in 5 minutes during market hours,
   something upstream is broken.
"""

from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class EventMetric:
    """One data point per pipeline event."""
    ticker:         str
    event_time:     datetime   # when MongoDB recorded the change (business time)
    publish_time:   datetime   # when Pub/Sub received the message
    ingestion_time: datetime   # when Cloud Function wrote to BigQuery
    is_duplicate:   bool = False
    is_late:        bool = False

    @property
    def processing_lag_ms(self) -> float:
        """Pub/Sub → Cloud Function latency in milliseconds."""
        return (self.ingestion_time - self.publish_time).total_seconds() * 1000


class PipelineMetrics:
    """
    Collects EventMetrics as the pipeline runs and provides summary statistics.
    One instance lives for the lifetime of a StockPipeline.
    """

    def __init__(self):
        self._events: List[EventMetric] = []
        # High-watermark: latest event_time seen per ticker
        # Used to detect late/out-of-order arrivals
        self._watermarks: Dict[str, datetime] = {}

    def record(self,
               ticker: str,
               event_time: datetime,
               publish_time: datetime,
               ingestion_time: datetime,
               is_duplicate: bool = False) -> EventMetric:
        """
        Record one pipeline event. Automatically detects late arrivals
        by comparing event_time against the per-ticker high-watermark.
        """
        is_late = False
        if not is_duplicate:
            watermark = self._watermarks.get(ticker)
            if watermark and event_time < watermark:
                is_late = True
            else:
                self._watermarks[ticker] = event_time

        metric = EventMetric(
            ticker=ticker,
            event_time=event_time,
            publish_time=publish_time,
            ingestion_time=ingestion_time,
            is_duplicate=is_duplicate,
            is_late=is_late,
        )
        self._events.append(metric)
        return metric

    # ─────────────────────────────────────────────────────────────────────
    # SUMMARY STATISTICS
    # ─────────────────────────────────────────────────────────────────────

    def lag_stats(self) -> Dict[str, float]:
        """Processing lag stats across all non-duplicate events (milliseconds)."""
        lags = [e.processing_lag_ms for e in self._events if not e.is_duplicate]
        if not lags:
            return {"avg_ms": 0.0, "max_ms": 0.0, "min_ms": 0.0, "p95_ms": 0.0}

        lags_sorted = sorted(lags)
        p95_idx = int(len(lags_sorted) * 0.95)

        return {
            "avg_ms": sum(lags) / len(lags),
            "max_ms": max(lags),
            "min_ms": min(lags),
            "p95_ms": lags_sorted[min(p95_idx, len(lags_sorted) - 1)],
        }

    def event_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for e in self._events:
            if not e.is_duplicate:
                counts[e.ticker] = counts.get(e.ticker, 0) + 1
        return counts

    def late_event_count(self) -> int:
        return sum(1 for e in self._events if e.is_late)

    def duplicate_count(self) -> int:
        return sum(1 for e in self._events if e.is_duplicate)

    def data_freshness(self) -> Dict[str, str]:
        """Latest event_time seen per ticker (human-readable)."""
        return {
            ticker: ts.strftime("%a %Y-%m-%d %H:%M:%S")
            for ticker, ts in self._watermarks.items()
        }

    def full_report(self) -> Dict:
        """Single dict with everything — suitable for logging or display."""
        lag = self.lag_stats()
        return {
            "total_events":       len(self._events),
            "events_processed":   len(self._events) - self.duplicate_count(),
            "duplicates_skipped": self.duplicate_count(),
            "late_events":        self.late_event_count(),
            "lag_ms": {
                "avg":  round(lag["avg_ms"], 3),
                "max":  round(lag["max_ms"], 3),
                "min":  round(lag["min_ms"], 3),
                "p95":  round(lag["p95_ms"], 3),
            },
            "events_per_ticker": self.event_counts(),
            "data_freshness":    self.data_freshness(),
        }
