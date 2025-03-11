# Datasets and Data-Aware Scheduling in Airflow

Demo repository for the [Datasets and Data-Aware Scheduling in Airflow webinar](https://www.astronomer.io/events/webinars/datasets-and-data-aware-scheduling-in-airflow-video).

## How to run the demo

### Run the demo locally

1. Clone this branch of the repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed.
3. Run `astro dev start` to start the Airflow instance. The webserver with the Airflow UI will be available at `localhost:8080`. Log in with the credentials `admin:admin`.
4. Run the DAGs you want to experiment with. No external connections are needed for this demo.

The [`include`](include) directory contains two scripts to create dataset updates with the Airflow REST API. 

## Resources

- [Datasets and Data-Aware Scheduling in Airflow webinar](https://www.astronomer.io/events/webinars/datasets-and-data-aware-scheduling-in-airflow-video).
- [Datasets and data-aware scheduling in Airflow guide](https://www.astronomer.io/docs/learn/airflow-datasets/).