from airflow.sdk import dag, task, task_group, chain


@dag
def simple_task_group():

    @task
    def t0():
        pass

    @task_group
    def my_task_group():
        @task
        def t1():
            pass

        @task
        def t2():
            pass

        chain(t1(), t2())

    chain(t0(), my_task_group())


simple_task_group()
