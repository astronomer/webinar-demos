from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import time
import random
from textwrap import dedent


class SnowflakeOperator(BaseOperator):
    """Execute SQL in Snowflake."""

    ui_color = "#ededed"

    def __init__(
        self,
        *,
        sql,
        snowflake_conn_id: str = "snowflake_analytics",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.database = database
        self.schema = schema
        self.sql = sql

    def execute(self, context):
        self.log.info("Executing custom SQL in Snowflake.")
        time.sleep(random.randint(5, 20))
        self.log.info(f"SQL: {self.sql}")
        self.log.info(f"Database: {self.database}, Schema: {self.schema}")