from airflow.sdk import dag, task


def _get_divisor():
    import random

    return random.randint(0, 2)


@dag(tags=["bad_dag"])
def bad_dag_2():

    @task
    def get_number():
        import random

        return random.randint(0, 10)

    @task
    def divide_by_number(number):
        try:
            divisor = _get_divisor()
            return number / divisor
        except Exception as e:
            print(f"Error: {e}")

    _get_number = get_number()
    divide_by_number(_get_number)


bad_dag_2()
