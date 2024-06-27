from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from pendulum import datetime
import json 
import os


@dag(start_date=datetime(2024, 6, 1), schedule=[Dataset("data_summarized")], catchup=False)
def generate_product_insight():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("product_insight_sent")])

    @task 
    def get_product_summary_filepaths():
        files = os.listdir("include/feedback")

        full_filepaths = [
            os.path.join("include/feedback", file) for file in files
        ]

        return full_filepaths
    
    @task
    def get_product_summary(file_path):
        with open(file_path, "r") as f:
            return f.read()
        
    
    @task 
    def generate_product_insight(product_summary):
        from openai import OpenAI
        prompt = f"""
            Generate product insights from the following summary of Airflow features feedback: {product_summary}
            """
        client = OpenAI()
        chat_completion = client.chat.completions.create(
            model="gpt-4", messages=[{"role": "user", "content": prompt}]
        )

        # write to local text file with never more than 80 characters per line
        # with open("include/product_insight/.txt", "w") as f:
        #     f.write("\n".join(chat_completion.choices[0].message.content[i:i+80] for i in range(0, len(chat_completion.choices[0].message.content), 80)))

        return chat_completion.choices[0].message.content
    
    @task 
    def send_to_slack(insight):
        pass

    filepaths = get_product_summary_filepaths()
    product_summaries = get_product_summary.expand(file_path=filepaths)
    product_insights = generate_product_insight.expand(product_summary=product_summaries)
    send_to_slack.expand(insight=product_insights)


generate_product_insight()