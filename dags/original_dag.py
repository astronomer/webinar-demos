from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    start_date=None,
    schedule_interval=None,
    catchup=False,
    fail_stop=True,
)
def test_ruff_linter_dag():
    BashOperator(task_id="my_task", bash_command="echo 'Hello, World!'")

    @task
    def my_task(execution_date):
        print(execution_date)

    my_task()


test_ruff_linter_dag()


