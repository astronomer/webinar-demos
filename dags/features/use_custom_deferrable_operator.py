from airflow.sdk import dag, task
from include.custom_deferrable_operator import MyOperator

@dag
def use_custom_deferrable_operator():
    
    MyOperator(
        task_id="my_operator",
        wait_for_completion=True,
        poke_interval=20,
        deferrable=True,
    )


use_custom_deferrable_operator()