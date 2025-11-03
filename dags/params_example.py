import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, chain, task_group

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")


@dag(
    dag_display_name="Parameterized Queries 💡",
    schedule="@daily"
)
def params_example():

    @task_group
    def basic_usage():
        _parameters = SQLExecuteQueryOperator(
            task_id="parameters",
            conn_id=SNOWFLAKE_CONN_ID,
            show_return_value_in_logs=True,
            sql="SELECT %(tea_type)s",
            parameters={
                "tea_type": "fruit"
            }
        )

        _params = SQLExecuteQueryOperator(
            task_id="params",
            conn_id=SNOWFLAKE_CONN_ID,
            show_return_value_in_logs=True,
            sql="SELECT '{{ params.tea_type }}'",
            params={
                "tea_type": "fruit"
            }
        )

        _combine = SQLExecuteQueryOperator(
            task_id="combined",
            conn_id=SNOWFLAKE_CONN_ID,
            show_return_value_in_logs=True,
            sql="SELECT %(tea_type)s FROM {{ params.table }} LIMIT 1",
            parameters={
                "tea_type": "fruit"
            },
            params={
                "table": "ETL_DEMO.DEV.teas"
            }
        )

    @task_group
    def sql_injection():
        _security_params = SQLExecuteQueryOperator(
            task_id="params",
            conn_id=SNOWFLAKE_CONN_ID,
            show_return_value_in_logs=True,
            split_statements=True,
            sql="SELECT {{ params.tea_type }}",
            params={
                "tea_type": "''; CREATE TABLE ETL_DEMO.DEV.ohoh AS SELECT 1 AS some_number; --"
            }
        )

        _security_parameters = SQLExecuteQueryOperator(
            task_id="parameters",
            conn_id=SNOWFLAKE_CONN_ID,
            show_return_value_in_logs=True,
            split_statements=True,
            sql="SELECT %(tea_type)s",
            parameters={
                "tea_type": "''; CREATE TABLE ETL_DEMO.DEV.ohoh AS SELECT 1 AS some_number; --"
            }
        )

        chain(_security_params, _security_parameters)

    chain(basic_usage(), sql_injection())


params_example()
