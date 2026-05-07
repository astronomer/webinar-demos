from airflow.sdk import dag, task


@dag(tags=["simple_example_dag_test"], params={"my_param": 23})
def my_dag_task_flow():
    @task
    def print_info(**context):
        from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
        from airflow.sdk import Variable, Connection

        print("Logical date: ", context["logical_date"])
        print(f"my_param: {context['params']['my_param']}")

        my_variable = Variable.get("my_variable")
        print(f"my_variable: {my_variable}")

        my_conn = AwsGenericHook(aws_conn_id="my_aws_conn")
        print(f"my_conn: {my_conn}")

    print_info()


dag_object = my_dag_task_flow()

if __name__ == "__main__":
    from pendulum import datetime

    conn_path = "dags/simple_example_dag_test/my_connections.yaml"
    variables_path = "dags/simple_example_dag_test/my_variables.yaml"

    dag_object.test(
        logical_date=datetime(2025, 12, 15),
        conn_file_path=conn_path,
        variable_file_path=variables_path,
        run_conf={"my_param": 23},
    )
