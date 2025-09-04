from airflow.sdk import dag, task, Asset


@dag
def my_upstream_dag():
    @task(outlets=[Asset("x-asset1")])
    def task_1():
        return 1

    @task(outlets=[Asset("x-asset2")])
    def task_2():
        return 2
    
    task_1() >> task_2()


my_upstream_dag()


@dag(
    schedule=(
        (Asset("x-asset1") | Asset("x-asset2"))
        & (Asset("x-asset3") | Asset("x-asset4"))
    ),
)
def conditional_asset_schedule():
    @task
    def say_hello() -> None:
        import time

        time.sleep(10)
        print("Hello")

    say_hello()


conditional_asset_schedule()
