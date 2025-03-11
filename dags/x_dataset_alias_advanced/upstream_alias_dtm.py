"""
### Dataset Alias implementation - Upstream
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from pendulum import datetime
import logging
import time

t_log = logging.getLogger("airflow.task")

my_alias_name = "my_alias_dtm"


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["x_dataset_alias_advanced"],
)
def upstream_alias_dtm():

    @task
    def upstream_task():
        return ["bucket-1", "bucket-2", "bucket-3", "bucket-4"]

    @task(
        outlets=[DatasetAlias(my_alias_name)],
        map_index_template="{{ my_map_index_template }}",
    )
    def dynamic_dataset_creation(my_bucket):

        if my_bucket == "bucket-2":
            time.sleep(10)

        uri = f"s3://{my_bucket}"

        yield Metadata(
            Dataset(uri),
            extra={"k": "v"},  # extra has to be provided, can be {}
            alias=my_alias_name,
        )

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_map_index_template"] = f"Updated the {uri} dataset!"

    dynamic_dataset_creation.expand(my_bucket=upstream_task())


upstream_alias_dtm()
