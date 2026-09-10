from datetime import timedelta

from airflow.sdk import dag, task
from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)

from include.callback_functions import custom_async_callback


@dag(
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=10),
        callback=AsyncCallback(
            custom_async_callback,
            kwargs={"alert_type": "time_exceeded", "dag_id": "deadline_alerts_dag"},
        ),
    ),
)
def async_deadline_alert_dag():
    @task
    def print_hello():
        import time

        time.sleep(15)
        print("Hello, World!")

    print_hello()


async_deadline_alert_dag()