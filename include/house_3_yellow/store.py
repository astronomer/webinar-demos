import sqlite3

DB_PATH = "/tmp/house_3_yellow.db"


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders "
        "(dag_id TEXT, partition_key TEXT, utc_hour TEXT, amount REAL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS revenue "
        "(dag_id TEXT, hour TEXT, revenue REAL, run_id TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hourly_revenue "
        "(dag_id TEXT, hour TEXT, revenue REAL, run_id TEXT, PRIMARY KEY (dag_id, hour))"
    )
    return conn


def order_amount(utc_hour: int) -> float:
    return 1000.0 + 37.0 * utc_hour
