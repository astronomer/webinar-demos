"""
Third demo: watchers for the ``returns`` table, a source with no events and no offsets.

Kafka remembers how far a consumer group has read. A table doesn't, so the watcher has to remember
the last row it reported. Both triggers poll ``max(id)`` every few seconds and fire with the range
of new row ids. They differ only in where that cursor lives.

- ``InMemoryReturnsTrigger`` (demo3-before): a local variable in ``run()``. It's gone whenever the
  triggerer restarts (a deploy, a crash, a move to another triggerer), and the watcher then reports
  every row again.
- ``StatefulReturnsTrigger`` (demo3): the asset state store (Airflow 3.3), saved in the metadata
  database per asset. It survives restarts, and tasks of the Dags that use the asset can read it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


def fetch_max_id(conn_id: str) -> int:
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_first("SELECT COALESCE(MAX(id), 0) FROM returns")[0]


class InMemoryReturnsTrigger(BaseEventTrigger):
    """demo3-before: fires when new returns land. Keeps the last row id in memory."""

    def __init__(self, *, conn_id: str = "shop_db", poll_interval: float = 5.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {"conn_id": self.conn_id, "poll_interval": self.poll_interval},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        last_seen_id = 0  # lives as long as this run() call
        while True:
            self.log.info("Checking returns after id %s", last_seen_id)
            max_id = await asyncio.to_thread(fetch_max_id, self.conn_id)
            if max_id > last_seen_id:
                yield TriggerEvent({"from_id": last_seen_id + 1, "to_id": max_id})
                last_seen_id = max_id
            await asyncio.sleep(self.poll_interval)


class StatefulReturnsTrigger(InMemoryReturnsTrigger):
    """demo3: same watcher, with the last row id in the asset state store."""

    async def run(self) -> AsyncIterator[TriggerEvent]:
        last_seen_id = await self.asset_state_store.aget("last_seen_id", default=0)
        while True:
            self.log.info("Checking returns after id %s", last_seen_id)
            max_id = await asyncio.to_thread(fetch_max_id, self.conn_id)
            if max_id > last_seen_id:
                yield TriggerEvent({"from_id": last_seen_id + 1, "to_id": max_id})
                # Saved right after the yield, as early as the trigger can. The yield returns once
                # the triggerer has queued the event, and the event reaches the database on the
                # triggerer's next sync, about a second later. A crash before this line reports the
                # rows again; a crash in that second skips them. Keep the task safe to run twice.
                await self.asset_state_store.aset("last_seen_id", max_id)
                last_seen_id = max_id
            await asyncio.sleep(self.poll_interval)
