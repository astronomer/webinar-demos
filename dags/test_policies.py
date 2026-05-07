from airflow.sdk import dag, task


@dag(
    tags=["test_policies"]
)
def test_policies_dag():
    @task
    def test_policies():
        raise Exception("Test exception")

    test_policies()


test_policies_dag()
