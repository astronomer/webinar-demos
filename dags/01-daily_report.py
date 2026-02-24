"""
Generates synthetic booking data and refreshes the daily planet report.

**generate_bookings** inserts `n_bookings` bookings (+ matching payments) per run.
Each booking i gets a `booked_at` timestamp of (ds - (100 + i) days), so with the
default of 5 bookings they land 100–104 days before the execution date. Customers,
routes, and promo codes (20% chance) are picked randomly; passengers cycle 1→2→3→4;
departure is 3–60 days after booking, return is 3–399 days after departure, and
payment is recorded 5–59 minutes after booking.

**build_report** upserts per-planet aggregates for `ds` into `daily_planet_report`,
covering all bookings made up to and including the execution date. Trips whose
return date is still in the future are counted as active; past ones as completed.
"""

import pendulum
from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, dag, chain, Asset

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"


@dag(
    schedule="@daily",
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
    params={
        "n_bookings": Param(5, type="integer", description="Number of new bookings to generate for this run")
    },
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
    doc_md=__doc__,
)
def daily_report():

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

    chain(_generate, _report)


daily_report()
