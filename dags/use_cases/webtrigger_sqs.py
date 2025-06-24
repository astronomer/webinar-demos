from airflow.sdk import dag, task, chain
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
import os
from include.utils import get_random_user_profile

QUEUE_URL = os.getenv(
    "SQS_QUEUE", default="https://sqs.<region>.amazonaws.com/<account>/<queue>"
)


@dag(start_date=datetime(2025, 5, 1), schedule="@continuous", max_active_runs=1, params={"num_messages": 1})
def web_trigger_sqs():

    @task
    def sleep(**context):
        import time
        import random

        if context["params"]["num_messages"] == 1:
            sleep_time = random.randint(20, 120)
            print(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
        else:
            print("Manual trigger detected, skipping sleep.")

    @task
    def send_message_to_sqs(queue_url: str, aws_conn_id: str = "aws_default", **context):
        import json

        num_messages = context["params"]["num_messages"]

        for i in range(num_messages):
            message_body = get_random_user_profile()

            sqs_hook = SqsHook(aws_conn_id=aws_conn_id)

            response = sqs_hook.send_message(
                queue_url=queue_url, message_body=json.dumps(message_body)
            )

            print(f"Message sent to SQS. Response: {response}")

        

    chain(sleep(), send_message_to_sqs(queue_url=QUEUE_URL))


web_trigger_sqs()