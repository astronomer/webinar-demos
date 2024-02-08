# DAG writing for data engineers and data scientists - webinar demo

This repository contains the code for the webinar demo shown in DAG writing for data engineers and data scientists.

## Content

This repository contains:

- A DAG showing bad Airflow practices [`dags/bad_examples/bad_dag.py`](dags/bad_examples/bad_dag.py).
- A DAG showing how the bad DAG can be improved to use good Airflow practices [`dags/good_examples/good_dag.py`](dags/good_examples/good_dag.py).
- A script to dynamically generate DAG files from a JSON config using the files in [`include/dynamic_dag_generation`](include/dynamic_dag_generation).
- An example [CI/CD workflow](.github/workflows/deploy_to_astro.yaml) using GitHub Actions to test and deploy DAGs to [Astro](https://www.astronomer.io/try-astro).

- A small functional data pipeline ingesting information about sales, customer feedback and customer data from a dog toy company. To learn how to run this pipeline, see the section below.

## How to run this repository

1. Clone the repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed and that [Docker](https://www.docker.com/products/docker-desktop) is running.
3. Copy the `.env.example` file to a new file called `.env` and fill in your Snowflake and AWS connection details.
4. Create a new bucket in your AWS account with a folder called `ingest`.
5. Copy the 3 folders containing once CSV each from [`include/data_generation/data/ingest`](include/data_generation/data/ingest) to the `ingest` folder in your bucket. You can generate more data by running the[`include/data_generation/generate_sample_data.py`](include/data_generation/generate_sample_data.py) script.
6. Create a new databse in your Snowflake account called `HAPPYWOOFSDWH` with a schema called `HAPPYWOOFSDEV`.
7. In your Snowflake account create 3 new stages in order to be able to run the `load_to_snowflake` DAG using the SQL below.

    ```sql
    CREATE STAGE sales_reports_stage
    URL = 's3://<your-bucket-name>/load/sales_reports/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

    CREATE STAGE customer_feedback_stage
    URL = 's3://<your-bucket-name>/load/customer_feedback/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

    CREATE STAGE customer_data_stage
    URL = 's3://<your-bucket-name>/load/customer_data/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
    ```

8. In your Snowflake account create one new table called `TESTER_DOGS_TABLE` and fill it with the data from [`include/data_generation/data/dog_profiles.csv](include/data_generation/data/dog_profiles.csv).
9. Run `astro dev start` to start the Airflow webserver and scheduler.
10. Navigate to `localhost:8080` in your browser to see the Airflow UI.
11. Unpause all DAGs. The first run of the `ingest` DAGs will automatically start and trigger downstream DAGs via [Datasets](https://docs.astronomer.io/learn/airflow-datasets).


## Using `dag.test()`

Before running `dag.test()` on DAGs that import modules from the `include` folder, you need to add the `include` folder to the `PYTHONPATH` environment variable. You can do this by running the following command in the terminal:

```bash
export PYTHONPATH="<absolute-path-to-your-astro-project>:$PYTHONPATH"
```