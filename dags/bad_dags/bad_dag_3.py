"""CAVE: This DAG is intentionally bad! It is monolithic!"""

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import os


@dag(
    tags=["bad_dag"],
    params={
        "toy_of_interest": "Carrot Plushy",
    },
)
def bad_dag_3():

    @task
    def task1(**context):

        df1 = pd.read_csv(
            "include/example_data/customer_feedback.csv"
        )
        df2 = pd.read_csv(
            "include/example_data/customer_data.csv"
        )

        list_of_comments = list(df1["Comments"].unique())

        df2["PurchasesPerDog"] = df2["NumPreviousPurchases"] / df2["NumberOfDogs"]
        

        df3 = pd.read_csv(
            "include/example_data/sales_reports.csv"
        )

        for comment in list_of_comments:
            print(comment)

        print(df1)

        toy_of_interest = context["params"]["toy_of_interest"]

        df3 = df3.groupby("ProductName").agg({"QuantitySold": "sum"})

        print(
            f"The average number of purchases per toy is {df3['QuantitySold'].mean()}"
        )

        print(df3)

        print(
            f"The average number of purchases for the toy {toy_of_interest} is {df3.loc[toy_of_interest, 'QuantitySold']}"
        )

        return df3.loc[toy_of_interest, 'QuantitySold']

    task1()


bad_dag_3()