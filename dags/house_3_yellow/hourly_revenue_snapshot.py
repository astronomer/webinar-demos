from datetime import timezone
from zoneinfo import ZoneInfo

import pendulum

from airflow.sdk import dag, task

from include.house_3_yellow.store import connect, order_amount

BERLIN = ZoneInfo("Europe/Berlin")


@dag(
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 10, 25, 0, tz="Europe/Berlin"),
    end_date=pendulum.datetime(2026, 10, 25, 4, tz="Europe/Berlin"),
    catchup=True,
    max_active_runs=1,
    tags=["house_3_yellow"],
)
def hourly_revenue_snapshot():

    @task
    def save_revenue(logical_date=None, run_id=None):
        hour = logical_date.astimezone(BERLIN).strftime("%Y-%m-%d %H:00")
        revenue = order_amount(logical_date.astimezone(timezone.utc).hour)
        with connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO hourly_revenue VALUES (?, ?, ?, ?)",
                ("hourly_revenue_snapshot", hour, revenue, run_id),
            )
        print(f"Saved EUR {revenue:,.2f} for Berlin hour {hour}.")

    save_revenue()


hourly_revenue_snapshot()
