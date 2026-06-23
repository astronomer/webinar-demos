from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLColumnCheckOperator,
    SQLValueCheckOperator,
)
from airflow.sdk import chain, dag, task_group

@dag
def demo4():

    @task_group
    def source_data_checks():

        _check_planet_count = SQLValueCheckOperator(
            task_id="check_planet_count",
            conn_id="snowflake_astrotrips",
            sql="SELECT COUNT(*) FROM planets",
            pass_value=3,
        )

        _check_booking_columns = SQLColumnCheckOperator(
            task_id="check_booking_columns",
            conn_id="snowflake_astrotrips",
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

        chain(
            _check_planet_count,
            _check_booking_columns,
        )

    _bookings_per_planet = SQLExecuteQueryOperator(
        task_id="get_bookings_per_planet",
        conn_id="snowflake_astrotrips",
        sql=(
            "SELECT PLANET_NAME, COUNT(*) "
            "FROM BOOKINGS b "
            "JOIN ROUTES r ON b.route_id = r.route_id "
            "JOIN PLANETS p ON r.destination_id = p.planet_id "
            "GROUP BY 1"
        ),
    )

    chain(source_data_checks(), _bookings_per_planet)

demo4()
