from airflow.sdk import DAG, task


with DAG(
    dag_id="my_dag_traditional",
    tags=["simple_example_dag_test"]
) as dag_object:

    @task
    def print_hello():
        print("Hello, World!")

    print_hello()


if __name__ == "__main__":
    dag_object.test()
