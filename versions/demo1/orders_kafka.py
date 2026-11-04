"""First demo: a Dag runs the moment a paid order lands in Kafka, with the order as the event payload."""

from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

paid_trigger = MessageQueueTrigger(
    scheme="kafka",
    topics=["orders"],
    kafka_config_id="kafka_default",
    apply_function="include.filters.is_paid_order",
)

# Same watcher on Amazon SQS: change the scheme and pass that broker's own options.
# paid_trigger = MessageQueueTrigger(
#     scheme="sqs",
#     sqs_queue="https://sqs.eu-west-1.amazonaws.com/123456789012/orders",
# )

paid_orders = Asset("paid_orders", watchers=[AssetWatcher(name="paid_watcher", trigger=paid_trigger)])


@dag(schedule=[paid_orders], tags=["event-driven"])
def ship_orders():
    @task
    def ship(triggering_asset_events):
        # One run can carry several events, so loop. In Airflow 3.3.2 orders that arrive together
        # don't share a run: the first starts one, and the others come with the next run.
        for event in triggering_asset_events[paid_orders]:
            order = event.extra["payload"]  # what is_paid_order returned
            print(f"Shipping order {order['order_id']} to {order['customer']} ({order['amount']} USD)")

    ship()


ship_orders()
