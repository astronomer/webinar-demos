from airflow.sdk import dag, task, Param


@dag(
    params={
        "num": Param(type="integer", default=10),
    },
    tags=["good_dag"],
)
def modular_dag():

    @task
    def get_num(**context):
        return context["params"]["num"]

    @task
    def get_fibonacci_sequence(num) -> list[int]:
        from include.math_func import get_fibonacci_sequence
        fib_sequence = get_fibonacci_sequence(num)
        return fib_sequence

    _num = get_num()
    get_fibonacci_sequence(_num)


modular_dag()

