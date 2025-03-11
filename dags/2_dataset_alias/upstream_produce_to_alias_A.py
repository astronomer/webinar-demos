"""
### Dataset Alias implementation - Upstream
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")

my_alias_name = "my_alias_name2"


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["2_dataset_alias"],
)
def upstream_produce_to_alias_A():

    @task(outlets=[DatasetAlias(my_alias_name)])
    def attach_event_to_alias_metadata():
        char = "A"  # determined at runtime, for example based on upstream input
        yield Metadata(
            Dataset(f"x-dataset-{char}"),
            extra={"k": "v"},  # extra has to be provided, can be {}
            alias=my_alias_name,
        )

    attach_event_to_alias_metadata()

    # # Alternative way to attach a dataset event to a Dataset Alias
    # @task(outlets=[DatasetAlias(my_alias_name)])
    # def attach_event_to_alias_context(**context):
    #     char = "A"   # determined at runtime, for example based on upstream input
    #     outlet_events = context["outlet_events"]
    #     outlet_events[my_alias_name].add(
    #         Dataset(f"x-dataset-{char}"), extra={"k": "v"}
    #     )  # extra is optional

    # attach_event_to_alias_context()


upstream_produce_to_alias_A()
