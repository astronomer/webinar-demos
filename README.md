# Best practices for debugging your Apache Airflow® Dags

This is a companion project for the debugging best practices webinar, showcasing various common issues and how to work with Airflow in a local dev environment for proper debugging.

## Prerequisites

- Copy `.env.dist` to `.env`.
- Install the Astro CLI (https://docs.astronomer.io/astro/cli/install-cli).

## Dags and context

The Dags in this project model a data pipeline on DuckDB with SQL files stored in `include/sql`. The scenario is a fictional company called AstroTrips, selling space trips to planets.

- `setup` (`dags/setup.py`): prepares the database by running cleanup, schema, and fixtures SQL in order.
- `daily_report` (`dags/daily_report.py`): generates daily booking data, calculates the daily report, fetches the report rows, and prints a formatted summary to logs.

Both Dags use the `duckdb_astrotrips` connection (configured via an env variable) and load SQL templates via `template_searchpath` from `include/sql`.

The schema models planets, routes, customers, bookings, promo codes, and payments. The daily report aggregates passengers, trip status (active vs completed), and gross/discount/net revenue per planet for a given report date.

## Development container

This project comes with a [dev container definition](.devcontainer/devcontainer.json) to open and interact with the project in a container.

A dev container allows you to use a container as a fully featured development environment. Instead of installing dependencies, libraries, and tools directly on your local machine, everything required to work on this codebase is defined once and runs inside the container. This isolates dependencies to the project, prevents conflicts with your local setup, and ensures that development, testing, and CI environments behave exactly the same way.

This setup is built on the [Development Container Specification](https://containers.dev/), which provides a standard format for describing the environment without complex orchestration. It focuses on a simple, single-container configuration that works equally well for local development and remote environments. By utilizing reusable features and templates, this standard makes it easy to share and extend the environment, ensuring every contributor has the correct toolset from the moment they check out the code.

Modern editors like VS Code and PyCharm can read this configuration to automatically build and attach to the dev container. The editor's interface remains local and responsive, while the shell, debugger, and runtime operate entirely inside the container. This separation also allows you to install editor extensions and plugins that are scoped strictly to the project, giving you a tailored workspace without cluttering or modifying your global editor configuration.

## Run the project

The easiest way to run this project is by using the Astro CLI to either run it locally or export it to the Astro IDE to run it in the cloud.

## Run tests

```sh
astro dev pytest -a "--disable-warnings"
```

## Using MotherDuck (optional)

This project is configured to use DuckDB with a local database file stored in `include/astrotrips.duckdb`. While this setup is sufficient for this scenario, it has specific limitations:

- **No concurrent access:** The database cannot be written to by multiple concurrent processes.
- **No distributed processing:** Because the database is a local file, all Airflow tasks must run on the same node to access it. This works reliably with the Astro CLI local environment (which uses the `LocalExecutor` to spawn worker subprocesses within the scheduler container) or a single-worker setup. However, it will fail in a distributed environment with multiple distinct worker nodes.

To run this code in a distributed setup or enable concurrent access, you can easily switch to [MotherDuck](https://motherduck.com), a managed cloud service for DuckDB.

1. Sign up for a free account at [motherduck.com](https://motherduck.com).
2. Once logged in, create a new attached database named `astrotrips`.
3. Go to **Settings** -> **Integrations** -> **Access Tokens**.
4. Click **Create token**, keep the default settings, and select **Create token** in the popup window.
5. Copy the generated token and update the connection details in your `.env` file as follows:

```
AIRFLOW_CONN_DUCKDB_ASTROTRIPS='{
    "conn_type":"duckdb",
    "host":"md:astrotrips?motherduck_token=<YOUR_MOTHERDUCK_TOKEN>"
}'
```

> **Note:** Ensure you also update any other references to the local DuckDB file path, such as `include/connections.yaml` if applicable.
