from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from transformers import pipeline  # heavy imports at the top level

# top level DAG code!
postgres_db = PostgresHook(postgres_conn_id="postgres_default")

@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # deprecated parameter
    catchup=False,
    # no owner or retries specified
    tags=["webinar"],
    param={"toy_of_interest": "Carrot Plushy"},
)
def bad_dag():
    @task
    def t1(): # non-descriptive task name
        df = pd.read_csv("include/data_generation/data/customer_feedback3.csv")

        sentiment_scores = []

        for feedback in df["Comments"]:
            sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            )
            result = sentiment_pipeline(feedback)
            sentiment_scores.append(result[0]["score"])

        df = pd.read_csv("include/data_generation/data/customer_data3.csv")
        df["PurchasesPerDog"] = df["NumPreviousPurchases"] / df["NumberOfDogs"]
        df = df[df["NumberOfDogs"] > 3]
        average_purchases_per_dog = df["PurchasesPerDog"].mean()
        print(
            f"Average purchases per dog for customers with more than 3 dogs: {average_purchases_per_dog}"
        )

        ## hard to read and no comments!

        return sentiment_scores

    t1()

    @task
    def t2(**context):
        today = datetime.now() # not idempotent!
        print(today)
        sales_data = pd.read_csv("include/data_generation/data/sales_reports3.csv")
        sales_data["Date"] = pd.to_datetime(sales_data["Date"]).dt.tz_localize(None)
        sales_data = sales_data[sales_data["Date"] > today - pd.Timedelta(days=7)]
        total_sales = sales_data["Revenue"].sum()
        print(f"Total sales from the last 7 days: {total_sales}")

    t2()


bad_dag()
