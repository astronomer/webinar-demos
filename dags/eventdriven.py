from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

trigger = MessageQueueTrigger(
    aws_conn_id="aws_default",
    queue="https://sqs.us-east-1.amazonaws.com/339713022320/AirflowQueue",
    waiter_delay=5,
)

sqs_asset_queue = Asset(
    "sqs_asset_queue",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)

@dag(schedule=[sqs_asset_queue])
def event_driven_dag():
    
    @task
    def process_message(triggering_asset_events=None):
        for event in triggering_asset_events[sqs_asset_queue]:
            print(
                f"Processing message: {event.extra['payload']['message_batch'][0]['Body']}"
            )

    process_message()

event_driven_dag()