from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["Simple DAGs", "Deferrable"],
)
def deferrable_example():

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="my_ml_dag",
        conf={"message": "Hello from deferrable_example"},
        wait_for_completion=True,
        deferrable=True,  # setting the operator to deferrable mode
    )

    @task
    def downstream_task():
        return "Hi!"

    chain(trigger_dag, downstream_task())


deferrable_example()
