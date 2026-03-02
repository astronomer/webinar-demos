"""
Generates synthetic booking data, refreshes the planet report, then runs two
**soft** data-quality gates.

The gates in `report_quality_gates` are intentionally designed to fire on
this dataset.

Because of the `trigger_rule="all_done"` on the final task, the DAG run is marked
successful even when the gates fail, making this a "soft" DQ implementation.

We still fire notification, making this a good template for non-critical pipelines
where you want to be alerted about data issues but not fail the whole run.
"""

from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
    SQLThresholdCheckOperator,
)
from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, chain, dag, task_group

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"


@dag(
    schedule="@daily",
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
    params={
        "n_bookings": Param(5, type="integer", description="Number of new bookings to generate per run"),
    },
    doc_md=__doc__
)
def daily_report_soft_dq():

    _generate = SQLExecuteQueryOperator(
        task_id="generate_bookings",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="generate.sql",
        split_statements=True,
    )

    _report = SQLExecuteQueryOperator(
        task_id="build_report",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="report.sql",
    )

    @task_group(
        default_args={
            "on_failure_callback": DiscordNotifier(
                discord_conn_id="discord_default",
                text="""
                ```
                {{ exception }}
                ```
                """
            ),
        }
    )
    def report_quality_gates():

        # Fires because fixtures apply a promo code only ~20 % of the time.
        SQLCheckOperator(
            task_id="dq_all_bookings_have_promo",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="SELECT COUNT(*) = 0 FROM bookings WHERE promo_code IS NULL",
        )

        # Fires because Moon-route payments are ~4 000 USD/pax, well below 50k.
        SQLThresholdCheckOperator(
            task_id="dq_min_payment_above_50k",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="SELECT MIN(amount_usd) FROM payments",
            min_threshold=50_000,
            max_threshold=999_999_999,
        )

    # The only leaf task. trigger_rule="all_done" makes it run whether the
    # gates above passed (success) or fired (failed). Because a Dag run is
    # only marked failed when a LEAF task ends in failed/upstream_failed,
    # the Dag stays green even when the DQ checks fail.
    _done = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="all_done",
    )

    chain(_generate, _report, report_quality_gates(), _done)


daily_report_soft_dq()
