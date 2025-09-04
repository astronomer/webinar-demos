from airflow.sdk import dag, task, Param


@dag(
    params={
        "num": Param(type="integer", default=10),
    },
    tags=["bad_dag"],
)
def not_modular_dag():

    @task
    def get_num(**context):
        return context["params"]["num"]

    @task
    def get_fibonacci_sequence(num) -> list[int]:
        fib_sequence = []
        a, b = 0, 1
        for _ in range(num):
            a, b = b, a + b

            fib_sequence.append(a)

        return fib_sequence

    _num = get_num()
    get_fibonacci_sequence(_num)


not_modular_dag()
