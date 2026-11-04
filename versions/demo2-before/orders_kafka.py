"""Second demo, before the fix: a second watcher on the same topic, added the obvious way."""

from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

paid_trigger = MessageQueueTrigger(
    scheme="kafka",
    topics=["orders"],
    kafka_config_id="kafka_default",
    apply_function="include.filters.is_paid_order",
)
refund_trigger = MessageQueueTrigger(
    scheme="kafka",
    topics=["orders"],
    kafka_config_id="kafka_default",
    apply_function="include.filters.is_refund",
)

paid_orders = Asset("paid_orders", watchers=[AssetWatcher(name="paid_watcher", trigger=paid_trigger)])
refunds = Asset("refunds", watchers=[AssetWatcher(name="refund_watcher", trigger=refund_trigger)])


@dag(schedule=[paid_orders], tags=["event-driven"])
def ship_orders():
    @task
    def ship(triggering_asset_events):
        for event in triggering_asset_events[paid_orders]:
            order = event.extra["payload"]
            print(f"Shipping order {order['order_id']} to {order['customer']} ({order['amount']} USD)")

    ship()


@dag(schedule=[refunds], tags=["event-driven"])
def refund_orders():
    @task
    def refund(triggering_asset_events):
        for event in triggering_asset_events[refunds]:
            order = event.extra["payload"]
            print(f"Refunding order {order['order_id']}: {order['amount']} USD to {order['customer']}")

    refund()


ship_orders()
refund_orders()
