from datetime import timedelta

from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import dag, task, chain, Param


@dag
def HITLOperator_syntax_example():

    @task
    def upstream_task():
        return "Pick the Pokemon you want to start your journey with."

    _upstream_task = upstream_task()

    _hitl_task = HITLOperator(
        task_id="hitl_task",
        subject="Choose your starter",  # templatable
        body="{{ ti.xcom_pull(task_ids='upstream_task') }}",  # templatable
        options=["Bulbasaur", "Charmander", "Squirtle"],  # cannot be empty!
        defaults=["Charmander"],
        multiple=False,  # default: False
        params={
            "trainer_name": Param(
                "Ash",
                type="string",
            ),
        },
        execution_timeout=timedelta(minutes=5),  # default: None
    )

    @task
    def print_result(hitl_output):
        print(f"Trainer: {hitl_output['params_input']['trainer_name']}")
        print(f"Starter: {hitl_output['chosen_options']}")

    _print_result = print_result(_hitl_task.output)

    chain(_upstream_task, _hitl_task)


HITLOperator_syntax_example()
