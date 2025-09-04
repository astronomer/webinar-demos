from airflow.sdk import dag, task


@dag
def helper_dag_wait_30_seconds():
    @task
    def wait_30_seconds():
        import time

        time.sleep(30)

    wait_30_seconds()


helper_dag_wait_30_seconds()
