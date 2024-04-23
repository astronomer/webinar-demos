from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime


@dag(
    start_date=datetime(2023, 10, 18),
    schedule="@daily",
    tags=["Simple DAGs", "TaskFlow"],
    catchup=False,
)
def taskflowapi_vs_traditional():

    @task(retries=3)
    def say_hi_1(name: str = "") -> str:
        return f"Hi {name}!"

    say_hi_obj = say_hi_1("Astra")

    def say_hi_func(name: str = "") -> str:
        return f"Hi {name}!"

    say_hi_obj = PythonOperator(
        task_id="say_hi_2",
        python_callable=say_hi_func,
        op_args=["Astra"],
    )


taskflowapi_vs_traditional()
