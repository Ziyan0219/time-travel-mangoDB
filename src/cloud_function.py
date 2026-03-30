"""
Cloud Function Simulator
========================
Simulates the GCP Cloud Function that transforms MongoDB Change Events
into BigQuery-ready records.

WHAT A CLOUD FUNCTION DOES IN THIS PIPELINE:
  - Triggered automatically when a Pub/Sub message arrives
  - Serverless: scales to zero when idle, scales out under load
  - Stateless: each invocation is independent
  - Short-lived: should complete in seconds, not minutes

TRANSFORMATION RESPONSIBILITIES:
  1. Extract and normalize fields from the raw Change Event
  2. Add ingestion_time (when WE processed it, vs when MongoDB wrote it)
  3. Schema validation / type coercion
  4. Filter irrelevant operation types (e.g., we might skip deletes)

TWO IMPORTANT TIMESTAMPS:
  event_time    = cluster_time from MongoDB = when the price actually changed
                  This is "business time" — the truth about when it happened.

  ingestion_time = datetime.now() inside the function = when we processed it
                  This is "processing time" — useful for debugging pipeline lag.

  For time travel queries, we always use event_time.
  ingestion_time helps us detect if the pipeline is running behind.
"""

from datetime import datetime
from typing import Dict, Any

from src.mongo_simulator import ChangeEvent


def transform_change_event(event: ChangeEvent) -> Dict[str, Any]:
    """
    Core transformation: MongoDB ChangeEvent → BigQuery row.

    This is the only logic in the Cloud Function.
    It's intentionally simple — heavy processing belongs in Dataflow/Beam.

    Args:
        event: A ChangeEvent emitted by MongoDB's Change Stream

    Returns:
        A flat dict matching the BigQuery table schema
    """
    doc = event.full_document

    return {
        "ticker":         doc["ticker"],
        "price":          float(doc["price"]),
        "volume":         int(doc.get("volume", 0)),
        "event_time":     event.cluster_time,        # ← business time (use for time travel)
        "operation_type": event.operation_type,
        "ingestion_time": datetime.now(),            # ← processing time (use for pipeline monitoring)
    }
