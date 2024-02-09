"""
## DAG that ingests customer feedback and customer data, analyzes sentiment, 
and calculates purchases per dog.

This DAG analyzes customer feedback with a HuggingFace model and calculates 
the average number of purchases per dog.

There are no extra connections required for this DAG.

![A very good dog](https://place.dog/300/200)
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from typing import List
import pandas as pd
import logging
import os

from include.hugging_face_functions import get_sentiment_score

# how to use the Airflow task logger
task_logger = logging.getLogger("airflow.task")

# Set variables as env vars or at the start of the DAG file,
# you want to change the DAG code as little as possible
BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "include/data_generation/data/ingest/")
IMPORT_PATH_CUSTOMER_FEEDBACK = (
    f"{BASE_DATA_PATH}customer_feedback/customer_feedback3.csv"
)
IMPORT_CUSTOMER_DATA = f"{BASE_DATA_PATH}customer_data/customer_data3.csv"
IMPORT_PATH_SALES_REPORTS = f"{BASE_DATA_PATH}sales_reports/sales_reports3.csv"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,  # DAG docs in Markdown
    description="Sentiment analysis and purchases per dog",  # DAG description
    default_args={
        "owner": "Avery",
        "retries": 3,
        "retry_delay": 5,
    },  # retries and owner specified
    params={
        "toy_of_interest": Param(
            "Carrot Plushy",
            type="string",
            title="Core toy to report",
            description="Define which toy to generate a report on.",
            enum=[
                "Carrot Plushy",
                "ChewChew Train Dog Bed",
                "Where is the Ball? - Transparent Edition",
                "Stack of Artisinal Homework",
                "Post breakfast treats - calory free",
            ],
        ),
    },
    tags=["webinar"],
)
def good_dag():

    # ------------------ #
    # Sentiment Analysis #
    # ------------------ #

    @task
    def ingest_customer_feedback(
        import_path_customer_feedback: str,
    ) -> pd.DataFrame:  # descriptive task name
        """Ingest customer feedback data from a CSV file.
        Args:
            import_path_customer_feedback: The path to the CSV file.
        Returns:
            pd.DataFrame: The customer feedback data.
        """

        return pd.read_csv(import_path_customer_feedback)

    @task
    def fetch_comments_from_customer_feedback(
        df: pd.DataFrame,
    ) -> List[str]:  # type hints
        """Ingest customer feedback data from a CSV file.
        Args:
            df: The customer feedback data.
        Returns:
            List[str]: The list of comments from the customer feedback data.
        """

        list_of_comments = list(df["Comments"].unique())

        return list_of_comments

    @task
    def analyze_sentiment(comment: str) -> float:
        """
        Analyze sentiment of customer feedback using a HuggingFace model.

        Args:
            comment: The input comment.
        Returns:
            List[float]: The sentiment scores for each feedback.
        """

        sentiment = get_sentiment_score(
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            text_input=comment,
        )

        task_logger.info(
            f"The comment {comment} has a sentiment score of {sentiment['sentiment_score']}"
        )

        return {
            "comment": comment,
            "sentiment_score": sentiment["sentiment_score"],
            "sentiment_label": sentiment["sentiment_label"],
        }

    @task
    def report_results(sentiment_scores: List[dict], df: pd.DataFrame) -> None:
        """
        Report the sentiment scores as well as how often that comment was given.
        Args:
            sentiment_scores: The sentiment scores for each comment.
            df: The customer feedback data.
        Returns:
            None
        """

        for score in sentiment_scores:
            comment = score["comment"]
            sentiment_score = score["sentiment_score"]
            count = df[df["Comments"] == comment].shape[0]
            task_logger.info(
                f"The comment {comment} has a sentiment score of {sentiment_score} and was given {count} times."
            )

    # ------------------------- #
    # Purchase per dog Analysis #
    # ------------------------- #

    @task
    def ingest_customer_data(import_path_customer_data: str) -> pd.DataFrame:
        """Ingest customer data from a CSV file.
        Args:
            import_path_customer_feedback: The path to the CSV file.
        Returns:
            pd.DataFrame: The customer data.
        """

        return pd.read_csv(import_path_customer_data)

    @task
    def calculate_purchases_per_dog(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the average number of purchases per dog.
        Args:
            df: The customer data.
        Returns:
            pd.DataFrame: The customer data with the average number of purchases per dog.
        """

        df["PurchasesPerDog"] = df["NumPreviousPurchases"] / df["NumberOfDogs"]

        return df

    # ------------------------- #
    # Purchase per toy Analysis #
    # ------------------------- #

    @task
    def ingest_sales_reports(import_path_sales_reports: str) -> pd.DataFrame:
        """Ingest sales reports from a CSV file.
        Args:
            import_path_sales_reports: The path to the CSV file.
        Returns:
            pd.DataFrame: The customer data.
        """

        return pd.read_csv(import_path_sales_reports)

    @task
    def calculate_purchases_per_toy(df: pd.DataFrame, **context) -> pd.DataFrame:
        """
        Calculate the average number of purchases per toy.
        Args:
            df: The sales reports.
        Returns:
            pd.DataFrame: The sales reports with the total number of purchases per toy.
        """

        toy_of_interest = context["params"]["toy_of_interest"]

        df = df.groupby("ProductName").agg({"QuantitySold": "sum"})

        task_logger.warn(
            f"The average number of purchases per toy is {df['QuantitySold'].mean()}"
        )

        task_logger.critical(
            f"The average number of purchases for the toy {toy_of_interest} is {df.loc[toy_of_interest, 'QuantitySold']}"
        )

        return df

    # ------------ #
    # Dependencies #
    # ------------ #

    # Sentiment Analysis
    ingest_customer_feedback_obj = ingest_customer_feedback(
        import_path_customer_feedback=IMPORT_PATH_CUSTOMER_FEEDBACK
    )

    fetch_comments_from_customer_feedback_obj = fetch_comments_from_customer_feedback(
        ingest_customer_feedback_obj
    )

    analyze_sentiment_obj = analyze_sentiment.expand(
        comment=fetch_comments_from_customer_feedback_obj
    )
    report_results(analyze_sentiment_obj, ingest_customer_feedback_obj)

    # Purchase per dog Analysis
    calculate_purchases_per_dog(
        ingest_customer_data(import_path_customer_data=IMPORT_CUSTOMER_DATA)
    )

    # Purchase per toy Analysis
    calculate_purchases_per_toy(
        ingest_sales_reports(import_path_sales_reports=IMPORT_PATH_SALES_REPORTS)
    )


good_dag_obj = good_dag()


if __name__ == "__main__":

    # conn_path = "include/connections_and_vars_for_dagtest/connections.yaml"
    # variables_path = "include/connections_and_vars_for_dagtest/variables.yaml"
    toy_of_interest = "Stack of Artisinal Homework"

    good_dag_obj.test(
        # execution_date=datetime(2025, 1, 1),
        # conn_file_path=conn_path,
        # variable_file_path=variables_path,
        run_conf={"toy_of_interest": toy_of_interest},
    )
