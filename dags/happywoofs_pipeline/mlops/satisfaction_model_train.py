"""
## Create features to train a regression model on to predict customer ratings
"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
import json
import os
import pandas as pd

with open("include/dynamic_dag_generation/ingestion_source_config.json", "r") as f:
    config = json.load(f)

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_de_team")
FEATURES_TABLE_NAME = os.getenv("FEATURES_TABLE_NAME", "FEATURES_TABLE")
BASE_FEATURES_TABLE_NAME = os.getenv("BASE_FEATURES_TABLE_NAME", "BASE_FEATURES")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[
        Dataset(f"snowflake://{source['source_name']}_table")
        for source in config["sources"]
    ],
    catchup=False,
    tags=["ML", "HappyWoofs"],
    default_args={"owner": "Haexli", "retries": 3, "retry_delay": 5},
    description="Feature creation for regression model",
    doc_md=__doc__,
)
def satisfaction_model_train():
    join_tables = SnowflakeOperator(
        task_id="create_file_format",
        sql=f"""
            CREATE OR REPLACE TABLE HAPPYWOOFSDWH.HAPPYWOOFSDEV.{BASE_FEATURES_TABLE_NAME} AS
            SELECT
                c.CUSTOMERID,
                c.NUMBEROFDOGS,
                c.NUMPREVIOUSPURCHASES,
                s.PRODUCTNAME,
                s.QUANTITYSOLD,
                s.REVENUE,
                f.DATE AS FEEDBACK_DATE,
                f.RATING,
                f.COMMENTS -- Optional, consider excluding if not using NLP techniques
            FROM
                HAPPYWOOFSDWH.HAPPYWOOFSDEV.CUSTOMER_FEEDBACK_TABLE f
            JOIN
                HAPPYWOOFSDWH.HAPPYWOOFSDEV.CUSTOMER_DATA_TABLE c ON f.CUSTOMERID = c.CUSTOMERID
            JOIN
                HAPPYWOOFSDWH.HAPPYWOOFSDEV.SALES_REPORTS_TABLE s ON f.PURCHASEID = s.PURCHASEID;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    @task
    def prepare_features() -> pd.DataFrame:
        """
        Prepare features for training the regression model.
        Returns:
            pd.DataFrame: DataFrame containing the features and target variable.
        """
        from sklearn.preprocessing import OneHotEncoder
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        query = """
            SELECT
                CUSTOMERID,
                NUMBEROFDOGS,
                NUMPREVIOUSPURCHASES,
                PRODUCTNAME,
                QUANTITYSOLD,
                FEEDBACK_DATE,
                RATING
            FROM
                HAPPYWOOFSDWH.HAPPYWOOFSDEV.BASE_FEATURES;
        """

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)

        df = cursor.fetch_pandas_all()

        df["FEEDBACK_DATE"] = pd.to_datetime(df["FEEDBACK_DATE"])
        df["MONTH"] = df["FEEDBACK_DATE"].dt.month
        df["DAY"] = df["FEEDBACK_DATE"].dt.day
        df["WEEKDAY"] = df["FEEDBACK_DATE"].dt.weekday

        ohe = OneHotEncoder()
        ohe.fit(df[["PRODUCTNAME"]])
        product_name_ohe = ohe.transform(df[["PRODUCTNAME"]])

        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    product_name_ohe.toarray(),
                    columns=ohe.get_feature_names_out(["PRODUCTNAME"]),
                ),
            ],
            axis=1,
        )

        return df

    @task
    def train_model(df: pd.DataFrame) -> None:
        """
        Train a regression model to predict customer ratings.
        Args:
            df (pd.DataFrame): DataFrame containing the features and target variable.
        """
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_squared_error
        from sklearn.preprocessing import StandardScaler

        X = df.drop(columns=["RATING", "CUSTOMERID", "PRODUCTNAME", "FEEDBACK_DATE"])
        y = df["RATING"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        scaler = StandardScaler()
        X_train[["NUMBEROFDOGS", "NUMPREVIOUSPURCHASES", "QUANTITYSOLD"]] = (
            scaler.fit_transform(
                X_train[["NUMBEROFDOGS", "NUMPREVIOUSPURCHASES", "QUANTITYSOLD"]]
            )
        )
        X_test[["NUMBEROFDOGS", "NUMPREVIOUSPURCHASES", "QUANTITYSOLD"]] = (
            scaler.transform(
                X_test[["NUMBEROFDOGS", "NUMPREVIOUSPURCHASES", "QUANTITYSOLD"]]
            )
        )

        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        print(f"Mean Squared Error: {mse}")

    model_trained = train_model(prepare_features())

    chain(join_tables, model_trained)


satisfaction_model_train()
