from datetime import timedelta

from airflow.sdk import dag, task

@dag(tags=["task state store", "webinar"])
def task_state_store_simple():

    @task
    def tss_random_number(**context):
        import random

        my_num = context["task_state_store"].get("my_num")
        my_new_num = random.randint(0, 100)
        context["task_state_store"].set("my_num", my_new_num)

        print("My number in the previous try was: ", my_num)
        print("My new num is:", my_new_num)

    tss_random_number()

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def do_some_math(my_num, **context):
        import random
        import time
        my_squared_num = context["task_state_store"].get("my_squared_num")
        
        
        if my_squared_num:
            print(f"Previously stored my_squared_num: {my_squared_num}")
        
        else:
            my_squared_num = my_num ** 2
            print(f"Computed the square of my_num")
            context["task_state_store"].set("my_squared_num", my_squared_num)
            time.sleep(5)
            raise Exception("Oops out of memory, the worker died and the task failed!")
        
        my_squared_num_plus_one = my_squared_num + 1
        print(f"my_squared_num_plus_one is {my_squared_num_plus_one}")

    do_some_math(my_num=23)


task_state_store_simple()
