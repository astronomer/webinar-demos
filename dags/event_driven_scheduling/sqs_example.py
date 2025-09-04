from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task
import os

# Define the SQS queue URL
SQS_QUEUE = os.getenv("SQS_QUEUE_URL", default="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")

# Define a trigger that listens to an external message queue (AWS SQS in this case)
trigger = MessageQueueTrigger(
    aws_conn_id="aws_default",
    queue=SQS_QUEUE,
    waiter_delay=30,  # delay in seconds between polls
)

# Define an asset that watches for messages on the queue
sqs_queue_asset = Asset(
    "sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)


# Schedule the DAG to run when the asset is triggered
@dag(schedule=[sqs_queue_asset])
def sqs_example():
    @task
    def process_message(**context):
        # Extract the triggering asset events from the context
        triggering_asset_events = context["triggering_asset_events"]
        for event in triggering_asset_events[sqs_queue_asset]:
            # Get the message from the TriggerEvent payload
            print(
                f"Processing message: {event.extra["payload"]["message_batch"][0]["Body"]}"
            )

    process_message()


sqs_example()
