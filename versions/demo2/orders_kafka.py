"""Second demo, after the fix: both watchers share one Kafka consumer, and no event is dropped."""

from __future__ import annotations

from airflow.sdk import Asset, AssetWatcher, dag, task
from include.triggers.order_events import OrderEventsTrigger

paid_trigger = OrderEventsTrigger(event_type="paid", topics=["orders"], dlq_topic="orders_dlq")
refund_trigger = OrderEventsTrigger(event_type="refunded", topics=["orders"], dlq_topic="orders_dlq")

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
