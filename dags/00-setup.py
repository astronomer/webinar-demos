from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, chain

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"


@dag(
    tags=["astrotrips", "setup"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
)
def setup():

    _schema = SQLExecuteQueryOperator(
        task_id="schema",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="schema.sql",
        split_statements=True,
    )

    _truncate = SQLExecuteQueryOperator(
        task_id="truncate",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="truncate.sql",
        split_statements=True,
    )

    _fixtures = SQLExecuteQueryOperator(
        task_id="fixtures",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="fixtures.sql",
        split_statements=True,
    )

    chain(_schema, _truncate, _fixtures)


setup()
