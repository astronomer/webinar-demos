# Webinar repository - How to write better dags

This is the companion repository for the [How to write better dags](https://www.astronomer.io/events/webinars/how-to-write-better-dags-in-airflow-video/) webinar.
It contains a collection of dags showing different best practices and Airflow features.

## How to run this demo locally

1. Fork this repo and clone this branch (`how-to-write-better-dags`) to your local machine.

2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed and are at least on version 1.34.0 to run Airflow 3.

3. Copy the `.env_example` file to a new file called `.env` and add your information. Most dags do not require any additional configuration or connections but some, like the Kafka and SQS dags, do.

3. Start the Airflow project with 
   ```bash
   astro dev start
   ```

    This command starts 7 containers:
    - Postgres: Airflow's Metadata Database
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Dag Processor: The Airflow component responsible for parsing dags
    - API Server: The Airflow component responsible for serving the Airflow UI and API
    - Triggerer: The Airflow component responsible for triggering deferred tasks
    - Kafka: A local Kafka server with one topic `my_topic`. The connection `kafka_default` is configured to use this Kafka server.
    - postgres_data: A local Postgres database to interact with from within Airflow tasks. The connection `postgres_default` is configured to use this database.

4. Access the Airflow UI at `localhost:8080`. 
5. Run the dags and make changes to experiment with the features.