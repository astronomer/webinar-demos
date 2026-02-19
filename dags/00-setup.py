from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, chain

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"


@dag(
    tags=["astrotrips", "setup"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
)
def setup():

    _cleanup = SQLExecuteQueryOperator(
        task_id="cleanup",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="cleanup.sql",
        split_statements=True,
    )

    _schema = SQLExecuteQueryOperator(
        task_id="schema",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="schema.sql",
        split_statements=True,
    )

    _fixtures = SQLExecuteQueryOperator(
        task_id="fixtures",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="fixtures.sql",
        split_statements=True,
    )

    chain(_cleanup, _schema, _fixtures)


setup()
