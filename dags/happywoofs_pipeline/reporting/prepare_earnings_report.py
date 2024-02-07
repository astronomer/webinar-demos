"""
## Prepares the earnings report for a given toy

The toy can be selected in manual runs as an Airflow param. 
By default the report will be on our flagship product, the Carrot Plushy.
"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from include.helpers import (
    set_up_report_platform,
    execute_report_mailer,
    tear_down_report_platform,
)
import os

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_de_team")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("snowflake://sales_reports_table")],
    catchup=False,
    tags=["report"],
    default_args={"owner": "Cerberus", "retries": 3, "retry_delay": 5},
    description="Load data from S3 to Snowflake",
    doc_md=__doc__,
    params={
        "toy_to_report": Param(
            "Carrot Plushy",
            type="string",
            title="Core toy to report",
            description="Define which toy to generate a report on.",
            enum=[
                "Carrot Plushy",
                "ChewChew Train Dog Bed",
                "Where is the Ball? - Transparent Edition",
                "Stack of Artisinal Homework",
                "Post breakfast treats - calory free",
            ],
        ),
        "simulate_metric_fetch_failure": Param(
            False,
            type="boolean",
            title="Simulate metric fetch failure",
            description="Simulate a failure in the metric fetch set to True to see Setup/Teardown in Action.",
        ),
        "simulate_metric_mail_failure": Param(
            False,
            type="boolean",
            title="Simulate metric mail failure",
            description="Simulate a failure in the metric mailing set to True to see Setup/Teardown in Action.",
        ),
    },
)
def prepare_earnings_report():
    @task
    def set_up_internal_reporting_platform():
        r = set_up_report_platform()
        return r

    @task(
        retries=0,
    )
    def get_key_metrics(**context):
        key_metric = context["params"]["toy_to_report"]
        simulate_metric_fetch_failure = context["params"][
            "simulate_metric_fetch_failure"
        ]
        if simulate_metric_fetch_failure:
            raise Exception("Metric fetch failed!")
        return key_metric

    get_total_earnings = SnowflakeOperator(
        task_id=f"get_total_earnings",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT SUM(REVENUE) FROM sales_reports_table 
        WHERE PRODUCTNAME = '{{ ti.xcom_pull(task_ids='get_key_metrics') }}';
        """,
    )

    @task(retries=0)
    def send_mails_internal_platform(**context):
        simulate_failure = context["params"]["simulate_metric_mail_failure"]
        if simulate_failure:
            raise Exception("Metric mailing failed!")
        r = execute_report_mailer()
        return r

    @task
    def tear_down_internal_reporting_platform():
        r = tear_down_report_platform()
        return r

    set_up_internal_reporting_platform_obj = set_up_internal_reporting_platform()
    tear_down_internal_reporting_platform_obj = tear_down_internal_reporting_platform()

    tear_down_internal_reporting_platform_obj.as_teardown(
        setups=[set_up_internal_reporting_platform_obj]
    )

    chain(
        set_up_internal_reporting_platform_obj,
        get_key_metrics(),
        get_total_earnings,
        send_mails_internal_platform(),
        tear_down_internal_reporting_platform_obj,
    )


prepare_earnings_report()
