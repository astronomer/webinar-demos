from airflow.sdk import dag, task, chain
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from pendulum import datetime
import json
import random

KAFKA_TOPIC = "my_topic"


def producer_function():
    sample_messages = [
        {"message_id": 1, "content": "Hello from Airflow!"},
    ]

    for i, message in enumerate(sample_messages):
        message["random_value"] = random.randint(1, 100)

        yield (json.dumps(f"key_{i}"), json.dumps(message))


@dag(tags=["webinar", "event-driven"])
def kafka_producer_dag():

    @task
    def prepare_producer_task():
        print("Preparing to produce messages to Kafka topic...")
        return "ready"

    produce_messages = ProduceToTopicOperator(
        task_id="produce_messages_to_kafka",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=producer_function,
        poll_timeout=10,
    )

    @task
    def completion_task():
        print(f"Successfully produced messages to Kafka topic: {KAFKA_TOPIC}")
        return "completed"

    prepare_task = prepare_producer_task()
    complete_task = completion_task()

    chain(prepare_task, produce_messages, complete_task)


kafka_producer_dag()