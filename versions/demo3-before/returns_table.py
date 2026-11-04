"""Third demo, before the fix: a watcher on a table, with its cursor in memory. A triggerer restart reports every row again."""

from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, AssetWatcher, dag, task
from include.triggers.returns import InMemoryReturnsTrigger

returns = Asset("returns", watchers=[AssetWatcher(name="returns_watcher", trigger=InMemoryReturnsTrigger())])


@dag(schedule=[returns], tags=["event-driven"])
def restock_returns():
    @task
    def restock(triggering_asset_events):
        hook = PostgresHook(postgres_conn_id="shop_db")
        for event in triggering_asset_events[returns]:
            new = event.extra["payload"]  # {"from_id": ..., "to_id": ...}
            rows = hook.get_records(
                "SELECT id, order_id, sku FROM returns WHERE id BETWEEN %s AND %s ORDER BY id",
                parameters=(new["from_id"], new["to_id"]),
            )
            for row_id, order_id, sku in rows:
                print(f"Restocking {sku} from order {order_id} (return {row_id})")

    restock()


restock_returns()
