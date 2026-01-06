from pathlib import Path

import duckdb
import pytest
from airflow import settings
from airflow.models import DagBag
from airflow.utils import db
from pendulum import datetime

_DUCKDB_FILE = "include/astrotrips.duckdb"
_DUCKDB_CONN_ID = "duckdb_astrotrips"


@pytest.fixture(scope="session", autouse=True)
def setup_airflow_db():
    settings.configure_orm()

    # initialize the DB schema and initially add Dags
    db.resetdb()
    yield


@pytest.fixture(autouse=True)
def setup_test_connections(monkeypatch):
    monkeypatch.setenv(
        f"AIRFLOW_CONN_{_DUCKDB_CONN_ID.upper()}",
        f'{{"conn_type": "duckdb", "host": {_DUCKDB_FILE}}}'
    )


def test_daily_report_pipeline_uses_temp_duckdb(tmp_path):
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("daily_report")
    assert dag is not None

    logical_date = datetime(2026, 1, 1)
    dag.test(logical_date=logical_date)

    duckdb_path = Path(_DUCKDB_FILE)
    assert duckdb_path.exists(), f"DuckDB file not found at {duckdb_path}"

    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        report_date = logical_date.to_date_string()
        con.execute(
            """
            SELECT COUNT(*)
            FROM daily_planet_report
            WHERE report_date = CAST(? AS DATE)
            """,
            [report_date],
        )
        row = con.fetchone()
        assert row is not None and row[0] > 0, f"Expected at least one report row for {report_date}"
