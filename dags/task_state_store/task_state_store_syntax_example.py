from datetime import timedelta

from airflow.sdk import dag, task

@dag(tags=["task state store"])
def task_state_store_syntax_example():

    @task
    def tss_set_and_get(**context):
        import random

        my_num = context["task_state_store"].get("my_num")
        my_new_num = random.randint(0, 100)
        context["task_state_store"].set("my_num", my_new_num)

        print("My number in the previous try was: ", my_num)
        print("My new num is:", my_new_num)

    tss_set_and_get()

    @task
    def tss_set_get_delete(**context):
        context["task_state_store"].set("my_num", 23)
        print(context["task_state_store"].get("my_num"))
        context["task_state_store"].delete("my_num")
        print(context["task_state_store"].get("my_num"))

    tss_set_get_delete()

    @task
    def tss_clear(**context):
        context["task_state_store"].set("my_num_A", 23)
        context["task_state_store"].set("my_num_B", 19)
        print(context["task_state_store"].get("my_num_A"))
        print(context["task_state_store"].get("my_num_B"))
        context["task_state_store"].clear()
        print(context["task_state_store"].get("my_num_A"))
        print(context["task_state_store"].get("my_num_B"))

    tss_clear()

    @task
    def tss_mapped(my_word, **context):
        print(my_word)
        context["task_state_store"].set("my_word", my_word)

    tss_mapped.expand(my_word=["hi", "hello", "hola"])

    @task
    def tss_clear_mapped(my_word, **context):
        print(my_word)
        context["task_state_store"].set("my_word", my_word)
        if context["ti"].map_index == 0:
            context["task_state_store"].clear()

    tss_clear_mapped.expand(my_word=["hi", "hello", "hola"])

    @task
    def tss_clear_mapped_all(my_word, **context):
        print(my_word)
        context["task_state_store"].set("my_word", my_word)
        if context["ti"].map_index == 0:
            context["task_state_store"].clear()

    tss_clear_mapped_all.expand(my_word=["hi", "hello", "hola"])


    @task(retries=1, retry_delay=timedelta(seconds=5))
    def tss_get_set(**context):
        my_num = context["task_state_store"].get("my_num")
        if my_num:
            print(f"Previously stored my_num: {my_num}")
        else:
            context["task_state_store"].set("my_num", 23)
            raise Exception("Simulated failure to trigger retry.")

    tss_get_set()


task_state_store_syntax_example()
