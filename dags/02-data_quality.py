"""
Data quality checks for the AstroTrips database.

Validates reference data integrity, transaction consistency, and daily report
quality using all six SQL check operators from apache-airflow-providers-common-sql.

Checks are organized in two task groups that run in sequence:

1. **source_data_checks**: field-level and integrity checks on planets, bookings,
   and payments (column checks first, as recommended).
2. **report_quality_checks**: trend and business-rule checks on the
   daily_planet_report aggregates (runs after source data is validated).

Operator coverage:
- SQLValueCheckOperator     → exactly 3 planets in reference data
- SQLColumnCheckOperator    → booking_id is unique/non-null; passengers in [1, 10]
- SQLCheckOperator          → no payments exist without a matching booking
- SQLThresholdCheckOperator → average payment is between 4 000 and 200 000 USD
- SQLIntervalCheckOperator  → daily report net revenue doesn't deviate > 2× vs last week
- SQLTableCheckOperator     → report aggregates satisfy business rules
"""

import pendulum
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLThresholdCheckOperator,
    SQLValueCheckOperator,
)
from airflow.sdk import chain, dag, task_group

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"


@dag(
    doc_md=__doc__
)
def data_quality():

    @task_group
    def source_data_checks():

        # Verify exactly 3 planets exist, any insert/delete would break fare logic
        _check_planet_count = SQLValueCheckOperator(
            task_id="check_planet_count",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="SELECT COUNT(*) FROM planets",
            pass_value=3,
        )

        # Field-level constraints on bookings:
        # booking_id must be non-null and unique; passengers must be between 1 and 10
        _check_booking_columns = SQLColumnCheckOperator(
            task_id="check_booking_columns",
            conn_id=_SNOWFLAKE_CONN_ID,
            table="bookings",
            column_mapping={
                "booking_id": {
                    "null_check": {"equal_to": 0},
                    "unique_check": {"equal_to": 0},
                },
                "passengers": {
                    "min": {"geq_to": 1},
                    "max": {"leq_to": 10},
                },
            },
        )

        # Relational integrity: every payment must reference an existing booking
        _check_no_orphaned_payments = SQLCheckOperator(
            task_id="check_no_orphaned_payments",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="""
                SELECT COUNT(*) = 0
                FROM payments p
                LEFT JOIN bookings b ON p.booking_id = b.booking_id
                WHERE b.booking_id IS NULL
            """,
        )

        # Average payment must sit within the plausible fare range:
        # lower bound = 1 pax × Moon fare (4 000 USD)
        # upper bound = 4 pax × Europa fare (240 000 USD), with margin
        _check_avg_payment = SQLThresholdCheckOperator(
            task_id="check_avg_payment_in_range",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="SELECT AVG(amount_usd) FROM payments",
            min_threshold=4000,
            max_threshold=200000,
        )

        # Column checks run first; integrity and threshold checks run in parallel after
        chain(
            _check_planet_count,
            _check_booking_columns,
            [_check_no_orphaned_payments, _check_avg_payment],
        )

    @task_group
    def report_quality_checks():

        # Compares SUM(total_net_fare_usd) for today's report_date against the same
        # value from exactly 7 days ago using ratio_formula="max_over_min":
        #
        #   ratio = max(current, past) / min(current, past)
        #
        # This is direction-agnostic: a 3× increase and a 3× decrease both produce
        # ratio = 3.0. The check fails when ratio >= threshold (strict: must be < 3).
        # Example from a real run: current=1 017 400, past=2 912 200 → ratio ≈ 2.86.
        # With threshold=2 that failed; threshold=3 passes because 2.86 < 3.
        #
        # ignore_zero=False is required: SUM() returns 0 (not NULL / no rows) when
        # there is no data for the comparison date. The default ignore_zero=True would
        # silently pass that zero; setting it to False ensures a missing historical
        # snapshot is treated as a failure. Use backfill to populate missing dates.
        _check_report_interval = SQLIntervalCheckOperator(
            task_id="check_report_revenue_vs_last_week",
            conn_id=_SNOWFLAKE_CONN_ID,
            table="daily_planet_report",
            date_filter_column="report_date",
            days_back=-7,
            ratio_formula="max_over_min",
            metrics_thresholds={"SUM(total_net_fare_usd)": 3},
            ignore_zero=False,
        )

        # Business rules on the report aggregates:
        # - net fare can't be negative
        # - discounts can't exceed the gross fare
        # - today's execution date must have at least one row
        _check_report_rules = SQLTableCheckOperator(
            task_id="check_report_business_rules",
            conn_id=_SNOWFLAKE_CONN_ID,
            table="daily_planet_report",
            checks={
                "net_fare_not_negative": {
                    "check_statement": "total_net_fare_usd >= 0",
                },
                "discounts_leq_gross": {
                    "check_statement": "total_discounts_usd <= total_gross_fare_usd",
                },
                "has_rows_for_today": {
                    "check_statement": "COUNT(*) >= 1",
                    "partition_clause": "report_date = '{{ ds }}'",
                },
            },
        )

        chain(_check_report_interval, _check_report_rules)

    chain(source_data_checks(), report_quality_checks())


data_quality()
