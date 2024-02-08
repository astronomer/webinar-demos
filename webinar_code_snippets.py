
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator


    # ------------ #
    # Data Quality #
    # ------------ #

    data_quality_check = SQLColumnCheckOperator(
        task_id="data_quality_check",
        database="HAPPYWOOFSDWH",
        table="HAPPYWOOFSDEV.SALES_REPORTS_TABLE",
        column_mapping={
                "QuantitySold": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100},
                },
            },
        conn_id="snowflake_de_team",
    )