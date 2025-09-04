"""
Well-structured DAG that demonstrates good practices by breaking down 
monolithic tasks into smaller, focused components.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import os


@dag(
    params={
        "toy_of_interest": "Carrot Plushy",
    },
    tags=["good_dag", "data_processing"],
)
def good_dag_3():

    @task
    def load_customer_feedback():
        """Load customer feedback data from CSV file."""
        df = pd.read_csv("include/example_data/customer_feedback.csv")
        return df.to_dict('records')

    @task
    def load_customer_data():
        """Load customer data from CSV file."""
        df = pd.read_csv("include/example_data/customer_data.csv")
        return df.to_dict('records')

    @task
    def load_sales_reports():
        """Load sales reports data from CSV file."""
        df = pd.read_csv("include/example_data/sales_reports.csv")
        return df.to_dict('records')

    @task
    def extract_unique_comments(feedback_data):
        """Extract and print unique comments from customer feedback."""
        df = pd.DataFrame(feedback_data)
        unique_comments = list(df["Comments"].unique())
        
        print("Unique customer comments:")
        for comment in unique_comments:
            print(f"- {comment}")
            
        return unique_comments

    @task
    def process_customer_data(customer_data):
        """Calculate purchases per dog metric for customer data."""
        df = pd.DataFrame(customer_data)
        df["PurchasesPerDog"] = df["NumPreviousPurchases"] / df["NumberOfDogs"]
        
        print("Customer data with PurchasesPerDog metric:")
        print(df)
        
        return df.to_dict('records')

    @task
    def process_sales_data(sales_data):
        """Aggregate sales data by product name."""
        df = pd.DataFrame(sales_data)
        aggregated_sales = df.groupby("ProductName").agg({"QuantitySold": "sum"})
        
        print("Aggregated sales data by product:")
        print(aggregated_sales)
        
        avg_purchases = aggregated_sales['QuantitySold'].mean()
        print(f"The average number of purchases per toy is {avg_purchases}")
        
        return aggregated_sales.to_dict('index')

    @task
    def analyze_toy_performance(aggregated_sales, **context):
        """Analyze performance of specific toy of interest."""
        toy_of_interest = context["params"]["toy_of_interest"]
        
        if toy_of_interest in aggregated_sales:
            toy_sales = aggregated_sales[toy_of_interest]['QuantitySold']
            print(f"The average number of purchases for the toy {toy_of_interest} is {toy_sales}")
            return toy_sales
        else:
            print(f"Toy '{toy_of_interest}' not found in sales data")
            available_toys = list(aggregated_sales.keys())
            print(f"Available toys: {available_toys}")
            return 0

    _load_customer_feedback = load_customer_feedback()
    _customer_data = load_customer_data()
    _sales_data = load_sales_reports()
    
    _comments = extract_unique_comments(_load_customer_feedback)
    _processed_customers = process_customer_data(_customer_data)
    _processed_sales = process_sales_data(_sales_data)
    
    _final_result = analyze_toy_performance(_processed_sales)


good_dag_3()