"""
Google Cloud Pub/Sub Simulator
==============================
Simulates the publish-subscribe message bus between Change Streams and Cloud Functions.

WHY PUB/SUB EXISTS IN THIS PIPELINE:
  Without Pub/Sub, Change Streams would write directly to BigQuery.
  The problem: if BigQuery is slow or temporarily unavailable, events are lost.
  Pub/Sub acts as a durable buffer — messages persist until acknowledged.

  Additional benefits:
    - Fan-out: one publish → multiple subscribers (BigQuery writer, alerting, etc.)
    - Decoupling: Change Stream producer and Cloud Function consumer run independently
    - Backpressure: if Cloud Functions are overwhelmed, messages queue up safely
    - Retry: failed message processing is automatically retried

Real GCP Pub/Sub semantics simulated here:
  - Topics: named channels for publishing messages
  - Subscriptions: named consumers that each get their own copy of every message
  - At-least-once delivery: messages may be delivered more than once (idempotency matters)
  - Message ordering: preserved within a partition key (simulated as FIFO here)
"""

import queue
from datetime import datetime
from dataclasses import dataclass
from typing import List, Any, Optional


@dataclass
class PubSubMessage:
    """
    A Pub/Sub message envelope.
    The 'data' field carries the actual payload (our MongoDB ChangeEvent).
    In real GCP, data is base64-encoded bytes; here we keep the Python object.
    """
    data: Any               # The payload (ChangeEvent in our case)
    message_id: str         # Unique ID assigned by Pub/Sub
    publish_time: datetime  # When the message was published to the topic


class PubSubSubscription:
    """
    A subscription is a named consumer of a topic.
    Each subscription maintains its own independent message queue —
    so two subscriptions both receive every message published to the topic.
    """

    def __init__(self, name: str):
        self.name = name
        self._queue: queue.Queue = queue.Queue()

    def _receive(self, message: PubSubMessage):
        """Called by the Topic when a new message is published."""
        self._queue.put(message)

    def pull(self, max_messages: int = 100) -> List[PubSubMessage]:
        """
        Pull up to max_messages from the subscription queue.
        In real GCP, you'd call subscriber.pull() or use a push subscription
        that triggers a Cloud Function automatically.
        """
        messages = []
        for _ in range(max_messages):
            try:
                messages.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return messages

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()


class PubSubTopic:
    """
    A Pub/Sub topic is the central message channel.

    Publishes from Change Stream handler → fan-out to all subscriptions.
    In real GCP, this is a fully managed service with global distribution,
    message ordering guarantees, and replay capabilities.
    """

    def __init__(self, name: str):
        self.name = name
        self._subscriptions: List[PubSubSubscription] = []
        self._message_counter = 0
        self.total_published = 0

    def subscribe(self, name: Optional[str] = None) -> PubSubSubscription:
        """Create a new subscription on this topic."""
        sub_name = name or f"{self.name}-sub-{len(self._subscriptions)}"
        sub = PubSubSubscription(sub_name)
        self._subscriptions.append(sub)
        return sub

    def publish(self, data: Any) -> str:
        """
        Publish a message to this topic.
        Every active subscription receives a copy (fan-out).
        Returns the assigned message_id.
        """
        self._message_counter += 1
        msg_id = f"msg-{self._message_counter:06d}"

        message = PubSubMessage(
            data=data,
            message_id=msg_id,
            publish_time=datetime.now(),
        )

        # Fan-out: every subscriber gets their own copy
        for sub in self._subscriptions:
            sub._receive(message)

        self.total_published += 1
        return msg_id
