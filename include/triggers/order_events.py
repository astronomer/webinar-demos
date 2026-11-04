"""
Second demo (demo2): one Kafka consumer for every watcher on the ``orders`` topic.

``KafkaSharedStreamTrigger`` (apache-kafka provider 2.1.0, Airflow 3.3+) gives every trigger on the
same topics and connection one shared consumer in the triggerer. Each message is handed to every
watcher, and the offset is committed only once every watcher has moved past the message and the
asset events it fired are stored in the metadata database.

This subclass adds one thing: each watcher fires only for its own event type.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.apache.kafka.triggers.shared_stream import KafkaSharedStreamTrigger
from airflow.triggers.base import TriggerEvent
from airflow.triggers.shared_stream import reject_shared_stream_event


class OrderEventsTrigger(KafkaSharedStreamTrigger):
    """Fire for every order event of one ``event_type``, such as ``paid`` or ``refunded``."""

    def __init__(self, *, event_type: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type

    def serialize(self) -> tuple[str, dict[str, Any]]:
        # The triggerer rebuilds the trigger from these kwargs, so the subclass's own argument
        # must be in them. The shared stream key stays topics + connection, so all watchers share.
        _, kwargs = super().serialize()
        return f"{type(self).__module__}.{type(self).__qualname__}", {**kwargs, "event_type": self.event_type}

    async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
        async for value in shared_stream:
            try:
                order = json.loads(value)
            except ValueError:
                # Not JSON: refuse it. It goes to dlq_topic instead of being committed and lost.
                reject_shared_stream_event()
                continue
            if order.get("type") == self.event_type:
                yield TriggerEvent(order)
