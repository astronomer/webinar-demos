from zoneinfo import ZoneInfo

import pendulum

from airflow.sdk import dag, task

from include.house_3_yellow.store import connect, order_amount

BERLIN = ZoneInfo("Europe/Berlin")


@dag(
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Berlin"),
)
def hourly_revenue_snapshot_slide():

    @task
    def save_revenue(logical_date=None, run_id=None):
        hour = logical_date.astimezone(BERLIN).strftime("%Y-%m-%d %H:00")
        revenue = order_amount(logical_date.hour)
        with connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO hourly_revenue VALUES (?, ?, ?, ?)",
                ("hourly_revenue_snapshot", hour, revenue, run_id),
            )

    save_revenue()


hourly_revenue_snapshot_slide()
