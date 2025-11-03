## ELT with Snowflake and Apache Airflow® - Reference architecture

Welcome! 🚀

This project is an end-to-end pipeline showing how to implement an ELT pattern with [Snowflake](https://www.snowflake.com/en/) and [Apache Airflow®](https://airflow.apache.org/). The pipeline extracts data from a mocked internal API, loads the raw data into [AWS S3](https://aws.amazon.com/s3/), and then loads the data into a Snowflake table to run transformations creating reporting tables for a [Streamlit dashboard](https://www.streamlit.io/). The pipeline also includes data quality checks that send a notification to [Discord](https://discord.com/) if they fail.

You can use this project as a starting point to build your own pipelines for similar use cases. It is showcased in the webinar [Production-Ready ELT Pipelines with Airflow 3 and Snowflake](https://www.astronomer.io/events/webinars/elt-pipelines-with-airflow-3-and-snowflake-video/).

## Tools used

- [Apache Airflow®](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [AWS S3](https://aws.amazon.com/s3/)
- [Snowflake](https://www.snowflake.com/en/)

**Optional:**

One of the Dags contains data quality checks that send notifications to Discord if they fail. To disable, remove the `on_failure_callback` from the `additional_dq_checks` task group in the [load_to_snowflake](dags/3_load_to_snowflake.py) Dag.

A [Discord](https://discord.com/) server and a channel with a configured webhook is needed for the Discord notifications. See [.env.dist](./.env.dist) on how to setup the connection.

## How to setup the demo environment

Follow the steps below to set up the demo for yourself.

1. Install Astronomer's open-source local Airflow development tool, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview).
2. Log into your AWS account and create [a new empty S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html). Make sure you have a set of [AWS credentials](https://docs.aws.amazon.com/iam/) with `AmazonS3FullAccess` for this new bucket.
3. Sign up for a [free trial](https://trial.snowflake.com/?owner=SPN-PID-365384) of Snowflake. Create a database called `ETL_DEMO` with a schema called `DEV`, as well as a warehouse called `MY_WH`. You can use the following SQL commands to create these objects:

```sql
CREATE WAREHOUSE MY_WH;
CREATE DATABASE IF NOT EXISTS ETL_DEMO;
CREATE SCHEMA IF NOT EXISTS ETL_DEMO.DEV;
```

4. Create a role and give it the necessary permissions to access the Snowflake objects. You can use the following SQL commands to create the role and grant the necessary permissions:

```sql
CREATE ROLE etl_demo_role;

GRANT USAGE ON WAREHOUSE MY_WH TO ROLE etl_demo_role;
GRANT USAGE ON DATABASE ETL_DEMO TO ROLE etl_demo_role;
GRANT USAGE ON SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;

GRANT ALL PRIVILEGES ON SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ETL_DEMO.DEV TO ROLE etl_demo_role;
```

5. In your terminal run the following command to [generate a private RSA key using OpenSSL](https://docs.openssl.org/master/man1/openssl-genrsa/). Note that while there are other options to generate a key pair, Snowflake has [specific requirements for the key format](https://docs.snowflake.com/en/user-guide/key-pair-auth) and may not accept keys generated with other tools. Make sure to write down the key passphrase as you will need it later.

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
```

Generate the associated public key using the following command:

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

6. Create a user for the demo and set your **public** key from the `rsa_key_pub` file. You can do this by running the following SQL commands.

```sql
CREATE USER my_demo_user
    PASSWORD = '<PW>'
    DEFAULT_ROLE = etl_demo_role
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE etl_demo_role TO USER my_demo_user;

ALTER USER my_demo_user SET RSA_PUBLIC_KEY='<PUBLIC KEY>';
```

7. Create your stage in Snowflake. You can do this by running the following SQL command. Make sure to replace the placeholders with your own values.

```sql
USE DATABASE ETL_DEMO;
USE SCHEMA DEV;

CREATE OR REPLACE STAGE DEMO_STAGE
URL='s3://<YOUR BUCKET NAME>/tea-sales-stage/'
CREDENTIALS=(AWS_KEY_ID='<YOUR AWS ACCESS KEY>', AWS_SECRET_KEY='<YOUR AWS SECRET KEY')
FILE_FORMAT = (TYPE = 'CSV');

GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA etl_demo.dev TO ROLE etl_demo_role;
```

8. Fork this repository and clone the code locally.

9. (Optional) Create a Discord webhook for the channel to receive the data quality alerts.

### Run the project locally

1. Create a new file called `.env` in the root of the cloned repository and copy the contents of [.env.dist](./.env.dist) into it. Fill out the placeholders with your own credentials for Snowflake, AWS, and Discord.
2. In the root of the repository, run `astro dev start` to start up the following Docker containers. This is your local development environment. Note that after any changes to `.env` you will need to run `astro dev restart` for new environment variables to be picked up.
3. Access the Airflow UI at `localhost:8080` and follow the Dag running instructions in the [Running the Dags](#running-the-dags) section of this README.

### Run the project in the cloud

1. Sign up to [Astro](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) for free and follow the onboarding flow to create a deployment with default configurations.
2. Deploy the project to Astro using `astro deploy`. See [Deploy code to Astro](https://www.astronomer.io/docs/astro/deploy-code).
3. Set up your Slack, AWS and Snowflake connections, as well as all other environment variables listed in [`.env_example](.env_example) on Astro. For instructions see [Manage Airflow connections and variables](https://www.astronomer.io/docs/astro/manage-connections-variables) and [Manage environment variables on Astro](https://www.astronomer.io/docs/astro/manage-env-vars).
4. Open the Airflow UI of your Astro deployment and follow the steps in [Running the Dags](#running-the-dags).

## Running the Dags

1. Unpause all Dags in the Airflow UI by clicking the toggle to the left of the Dag name.
2. The `extract_from_api` Dag will start its first run automatically. All other Dags are scheduled based on Datasets to run as soon as the required data is available.

Optional: Set up a Streamlit app in Snowflake using the script in [include/streamlit_app.py](include/streamlit_app.py) to visualize the data.

## Next steps

If you'd like to build your own ETL pipeline with Snowflake, feel free adapt this repository to your use case. We recommend to deploy the Airflow pipelines using a [free trial](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) of Astro.
