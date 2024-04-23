from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
import random


@dag(
    start_date=datetime(2023, 10, 18),
    schedule="@daily",
    tags=["Simple DAGs", "branching"],
    catchup=False,
)
def branching_example():

    @task.branch
    def is_champion() -> str:
        is_champion = random.choice(
            [True, False]
        )  # logic to determine if champion exists
        if is_champion:
            return "champion_exists"  # this is the task_id of the task to run (can also be a list of task_ids)
        else:
            return "no_champion"  # this is the task_id of the task to run (can also be a list of task_ids)

    @task
    def champion_exists() -> str:
        return "Champion exists"

    @task
    def no_champion() -> str:
        return "No champion exists"

    @task(trigger_rule="none_failed")  # careful with downstream trigger rules!
    def always_run_downstream() -> str:
        return "This task always runs"

    chain(is_champion(), [champion_exists(), no_champion()], always_run_downstream())


branching_example()
