"""
### Dataset Alias implementation - Downstream on a Dataset

Just a regular DAG with a regular Dataset schedule.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import logging
import time


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset("x-dataset-A")],
    catchup=False,
    doc_md=__doc__,
    tags=["2_dataset_alias"],
)
def downstream_on_dataset_A():

    @task
    def downstream():
        time.sleep(10)
        pass

    downstream()


downstream_on_dataset_A()
