"""

## Start the fine-tuning pipeline by updating datasets

This DAG exists to start the pipeline for an example run simulating
an update to the training and validation datasets.
It is intended to be triggered manually.
Alternatively you can use the API script in the `api_scripts` directory.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime
import os
import time

_TRAIN_EXAMPLES_URI = os.getenv("TRAIN_EXAMPLES_LONG_URI")
_VALIDATION_EXAMPLES_URI = os.getenv("VALIDATION_EXAMPLES_SHORT_URI")


@dag(
    dag_display_name="🚀 0 - Start Fine-Tuning Pipeline",
    start_date=datetime(2024, 4, 1),
    schedule=None,
    catchup=False,
    tags=["helper", "use-case"],
    default_args={
        "retries": 0,
        "owner": "Astronomer",
    },
    description="Start the pipeline by updating datasets.",
)
def start_pipeline():

    @task(outlets=[Dataset(_TRAIN_EXAMPLES_URI)])
    def update_train_examples():
        return "Train examples dataset updated."

    @task(outlets=[Dataset(_VALIDATION_EXAMPLES_URI)])
    def update_validation_examples():
        time.sleep(5)
        return "Validation examples dataset updated."

    update_train_examples()
    update_validation_examples()


start_pipeline()
