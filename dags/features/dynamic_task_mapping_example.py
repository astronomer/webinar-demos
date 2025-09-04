from airflow.sdk import dag, task


@dag(tags=["webinar"])
def dynamic_task_mapping_example():

    @task
    def get_numbers():
        import random
        return random.sample(range(1, 100), random.randint(1, 10))

    @task(map_index_template="{{ my_idx }}")
    def process_number(num, exponent):

        from airflow.sdk import get_current_context
        context = get_current_context()
        context["my_idx"] = f"This task computes: {num}^{exponent}"

        return num**exponent

    _get_numbers = get_numbers()
    process_number.partial(exponent=2).expand(num=_get_numbers)


dynamic_task_mapping_example()
