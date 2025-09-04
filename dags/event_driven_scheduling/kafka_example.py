import json

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, Asset, AssetWatcher, task

# Define the Kafka queue URL
# Replace <your_kafka_host>, <port>, and <your_topic> with your Kafka
KAFKA_QUEUE = "kafka://kafka:9092/my_topic"


# Define a function to apply when a message is received
# This function will be called with the message as an argument
def apply_function(*args, **kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val


# Define a trigger that listens to an external message queue (Kafka in this case)
trigger = MessageQueueTrigger(
    queue=KAFKA_QUEUE,
    apply_function="dags.event_driven_dag.apply_function",
)

# Define an asset that watches for messages on the Kafka topic
kafka_topic_asset = Asset(
    "kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)


@dag(schedule=[kafka_topic_asset], tags=["webinar"])
def event_driven_dag():
    @task
    def process_message(**context):
        # Extract the triggering asset events from the context
        triggering_asset_events = context["triggering_asset_events"]
        for event in triggering_asset_events[kafka_topic_asset]:
            # Get the message from the TriggerEvent
            print(f"Processing message: {event}")

    process_message()


event_driven_dag()
