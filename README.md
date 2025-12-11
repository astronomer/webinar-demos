## From Apache Airflow® 2 to 3: How to plan your upgrade

This repository contains the demo for the [From Apache Airflow® 2 to 3: How to plan your upgrade](https://www.astronomer.io/events/webinars/upgrade-airflow-2-to-3-video/) webinar.

## Repository Content

### Migration Examples (`dags/`)

* **db_access_af3.py** - Dag that demonstrates how to access the metadata database in Airflow 3 using the Airflow REST API and Airflow Client. 
* **direct_db_access.py** - Dag contains several examples of direct metadata database access that is NOT possible in Airflow 3.
* **original_dag.py** - Dag to show the ruff linter in the demo.
* **daily_dag_cron_data_interval_timetable.py** - Dag that demonstrates the context elements created when using the CronDataIntervalTimetable. (`AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=True` for raw cron strings/ shorthands like `@daily`).
* **daily_dag_cron_trigger_timetable.py** - Dag that demonstrates the context elements created when using the CronTriggerTimetable. (`AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=False` for raw cron strings/ shorthands like `@daily`).

## Getting Started

## Run the Demo Locally

1. Fork this repo and clone this branch (`upgrade-airflow-2-to-3`) to your local machine.

2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed and are at least on version 1.38.1 to be able to run Airflow 3.

3. Start the Airflow 2 project with 
   ```bash
   astro dev start
   ```
   The Airflow 3 API-server with the Airflow UI will be available at `localhost:8080`.

4. Run the [ruff linter] on the `original_dag.py` to see the ruff linter pointing out issues with the legacy dag.

    ```bash
    # Check for breaking issues
    ruff check --preview --select AIR30 dags/original_dag.py

    # Check for non-breaking issues
    ruff check --preview --select AIR31 dags/original_dag.py

    # Fix all issues
    ruff check --preview --select AIR3 --unsafe-fixes --fix dags/original_dag.py
    ```

5. Explore the other Dags in the `dags/` directory to learn about replacing direct metadata database access with the Airflow REST API and Airflow Client and scheduling changes.