import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.sdk import dag, Param, chain

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")


@dag(dag_display_name="Add tea 🫖", render_template_as_native_obj=True)
def tea_maintenance():

    _enter_tea_details = HITLEntryOperator(
        task_id="enter_tea_details",
        subject="Please provide required information: ",
        params={
            "tea_name": Param("", type="string"),
            "tea_type": Param("", type="string"),
            "price": Param(9.99, type="number")
        }
    )

    _insert_tea_details = SQLExecuteQueryOperator(
        task_id="insert_tea_details",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            INSERT INTO ETL_DEMO.DEV.teas(TEA_ID, TEA_NAME, TEA_TYPE, PRICE, UPDATED_AT)
            SELECT 
                UUID_STRING(),
                %(tea_name)s,
                %(tea_type)s,
                CAST(%(price)s AS FLOAT),
                CURRENT_TIMESTAMP()
        """,
        parameters={
            "tea_name": "{{ task_instance.xcom_pull('enter_tea_details')['params_input']['tea_name'] }}",
            "tea_type": "{{ task_instance.xcom_pull('enter_tea_details')['params_input']['tea_type'] }}",
            "price": "{{ task_instance.xcom_pull('enter_tea_details')['params_input']['price'] }}"
        }
    )

    chain(_enter_tea_details, _insert_tea_details)


tea_maintenance()
