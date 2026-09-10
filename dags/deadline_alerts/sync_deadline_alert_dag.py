from datetime import timedelta

from airflow.sdk import dag, task
from airflow.sdk.definitions.deadline import (
    SyncCallback,
    DeadlineAlert,
    DeadlineReference,
)

from include.callback_functions import custom_sync_callback



@dag(
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=10),
        callback=SyncCallback(
            custom_sync_callback,
            kwargs={"alert_type": "time_exceeded", "dag_id": "deadline_alerts_dag"},
        ),
    ),
)
def deadline_alerts_sync_callback():
    @task
    def print_hello():
        import time

        time.sleep(15)
        print("Hello, World!")

    print_hello()


deadline_alerts_sync_callback()