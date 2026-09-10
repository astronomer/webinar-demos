from airflow.sdk import dag
from datetime import timedelta

from include.custom_operators import MyBasicMathOperator, MyResumableMathOperator


@dag(tags=["task state store"])
def resumable_math_operator_example():

    MyResumableMathOperator(
        task_id="resumable_math",
        first_number=21,
        second_number=2,
        operation="/",
        simulate_crash=True,
        retries=10,
        retry_delay=timedelta(seconds=5),
    )

    MyBasicMathOperator(
        task_id="normal_math",
        first_number=21,
        second_number=2,
        operation="*",
        retries=1,
        retry_delay=timedelta(seconds=5), 
    )


resumable_math_operator_example()