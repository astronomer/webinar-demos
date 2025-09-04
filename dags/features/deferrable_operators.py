"""
## Toy DAG showing how to turn on deferrable mode for an operator

The default value for many operators `deferrable` argument is:
conf.getboolean("operators", "default_deferrable", fallback=False),
"""

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


@dag
def deferrable_operators():
    trigger_dag_run = TriggerDagRunOperator(
        task_id="trigger_dag_run",
        trigger_dag_id="helper_dag_wait_30_seconds",
        wait_for_completion=True,
        poke_interval=20,
        deferrable=True,  # or set at the config level
    )

    @task
    def celebrate():
        print("The downstream DAG has finished running!")

    trigger_dag_run >> celebrate()


deferrable_operators()