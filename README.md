## # Upgrading to Airflow 3 Webinar

This repository contains the demo for the [Best practices for Upgrading to Airflow 3](https://www.astronomer.io/events/webinars/best-practices-for-upgrading-to-airflow-3-video/) webinar.

## Repository Content

### Migration Examples (`dags/`)

* **`my_legacy_dag.py`** - Legacy DAG that runs in Airflow 2 but not in Airflow 3, demonstrating deprecated syntax and patterns
* **`my_fixed_dag.py`** - Fixed version of the legacy DAG that works in Airflow 3, showing the necessary syntax updates
* **`my_direct_db_access_dag.py`** - Demonstrates bad practice of direct metadatabase access (works in <3.0, removed in 3.0)
* **`my_dag_using_the_rest_api.py`** - Shows the recommended approach using Airflow REST API to access metadata in Airflow 3

## Key Migration Changes Demonstrated

### Syntax Updates
- `from airflow.decorators` → `from airflow.sdk`
- `schedule_interval` → `schedule`
- `execution_date` → `logical_date`
- `days_ago()` → `datetime()`
- `Dataset` → `Asset`
- `BashOperator` import path updated

## Getting Started

## Run the Demo Locally

1. Fork this repo and clone this branch (`best-practices-for-upgrading-to-airflow-3`) to your local machine.

2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed and are at least on version 1.34.0 to be able to run Airflow 3.

3. Start the Airflow 2 project with 
   ```bash
   astro dev start
   ```
   The Airflow 3 webserver with the Airflow UI will be available at `localhost:8080` log in with `admin` as the username and password. Test out the two dags that work with Airflow 2.

4. Run `astro dev kill` to reset the project. 

5. Switch the Dockerfile to the Airflow 3 image.

6. Run `astro dev start` to start up the project with Airflow 3.

7. Run the 3 DAGs. The `my_direct_db_access_dag` will fail because direct DB access is not allowed in Airflow 3. Note that you will likely need to adjust the `HOST` variable in the `my_dag_using_the_rest_api` to be able to query your Airflow environment.

8. Install [ruff](https://docs.astral.sh/ruff/rules/#airflow-air) and use `ruff check dags/my_legacy_dag.py` to see the ruff linter pointing out issues with the legacy dag.