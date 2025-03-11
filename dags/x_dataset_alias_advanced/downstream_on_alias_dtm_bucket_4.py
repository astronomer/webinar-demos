"""
### Dataset Alias implementation - Downstream on Alias
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import logging
import time

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset("s3://bucket-4/")],
    catchup=False,
    doc_md=__doc__,
    tags=["x_dataset_alias_advanced"],
)
def downstream_on_alias_dtm_bucket_4():

    @task
    def downstream():
        time.sleep(10)
        pass

    downstream()


downstream_on_alias_dtm_bucket_4()
