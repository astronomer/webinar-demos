"""
### Dataset Alias implementation - Upstream

Direct update to a Dataset associated with a Dataset Alias.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")

@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["2_dataset_alias"],
)
def upstream_produce_dataset_A():

    @task(outlets=[Dataset("x-dataset-A")])
    def update_dataset():
        pass

    update_dataset()


upstream_produce_dataset_A()
