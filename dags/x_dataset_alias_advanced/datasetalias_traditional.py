"""
### Dataset Alias in a traditional operator
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from pendulum import datetime
import logging
from airflow.operators.bash import BashOperator

t_log = logging.getLogger("airflow.task")


my_alias_name = "my_alias_9"

# import the operator to inherit from
from airflow.models.baseoperator import BaseOperator


# custom operator producing to a dataset alias
class MyOperator(BaseOperator):
    """
    Simple example operator.
    :param my_bucket_name: The name of the bucket to use.
    """

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(self, my_bucket_name, *args, **kwargs):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.my_bucket_name = my_bucket_name

    # define the .pre_execute() method that runs before the execute method (optional)
    def pre_execute(self, context):
        # write to Airflow task logs
        self.log.info("Pre-execution step")

    # define the .execute() method that runs when a task uses this operator.
    # The Airflow context must always be passed to '.execute()', so make
    # sure to include the 'context' kwarg.
    def execute(self, context):

        my_uri = f"s3://{self.my_bucket_name}/my_file.txt"

        context["outlet_events"][my_alias_name].add(Dataset(my_uri))

        # the return value of '.execute()' will be pushed to XCom by default
        return "hi :)"

    # define the .post_execute() method that runs after the execute method (optional)
    # result is the return value of the execute method
    def post_execute(self, context, result=None):
        # write to Airflow task logs
        self.log.info("Post-execution step")


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["x_dataset_alias_advanced"],
)
def datasetalias_traditional():

    MyOperator(
        task_id="t1",
        my_bucket_name="my-bucket",
        outlets=[DatasetAlias(my_alias_name)],
    )

    def _attach_event_to_alias(context, result):
        run_id = context["run_id"]
        context["outlet_events"][my_alias_name].add(
            Dataset(f"s3://my-bucket/my_file_{run_id}.txt")
        )

    BashOperator(
        task_id="t2",
        bash_command="echo hi",
        outlets=[DatasetAlias(my_alias_name)],
        post_execute=_attach_event_to_alias,
    )


datasetalias_traditional()
