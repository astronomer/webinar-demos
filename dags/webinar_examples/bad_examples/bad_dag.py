from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
from transformers import (
    pipeline,
)  # heavy imports should be inside the function if not needed at the top level
import os


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    # default args missing, no retries
    params={
        "toy_of_interest": "Carrot Plushy",  # Param but without list of options
    },
    tags=["webinar"],
)
def bad_dag():

    @task
    def task1(**context):

        df1 = pd.read_csv(
            "include/data_generation/data/ingest/customer_feedback/customer_feedback3.csv"
        )
        df2 = pd.read_csv(
            "include/data_generation/data/ingest/customer_data/customer_data3.csv"
        )

        list_of_comments = list(df1["Comments"].unique())

        df2["PurchasesPerDog"] = df2["NumPreviousPurchases"] / df2["NumberOfDogs"]
        list_of_sentiments = []
        for comment in list_of_comments:

            sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            )

            result = sentiment_pipeline(comment)
            sentiment_score = result[0]["score"]

            list_of_sentiments.append(
                {"comment": comment, "sentiment_score": sentiment_score}
            )

        print(f"The comment {comment} has a sentiment score of {sentiment_score}")

        df3 = pd.read_csv(
            "include/data_generation/data/ingest/sales_reports/sales_reports3.csv"
        )

        for score in list_of_sentiments:
            comment = score["comment"]
            sentiment_score = score["sentiment_score"]
            breakpoint()
            count = df1[df1["Comments"] == comment].shape[0]
            print(
                f"The comment {comment} has a sentiment score of {sentiment_score} and was given {count} times."
            )

        toy_of_interest = context["params"]["toy_of_interest"]

        df = df.groupby("ProductName").agg({"QuantitySold": "sum"})

        print(f"The average number of purchases per toy is {df['QuantitySold'].mean()}")

        print(
            f"The average number of purchases for the toy {toy_of_interest} is {df.loc[toy_of_interest, 'QuantitySold']}"
        )

    task1()


bad_dag()
