from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from pendulum import datetime
import os


@dag(start_date=datetime(2024, 6, 1), schedule=[Dataset("data_filtered")], catchup=False)
def tag_feedback_with_product():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("data_tagged_with_product")])

    @task
    def get_high_quality_texts():
        file_names = os.listdir("include/text_storage")
        file_paths = [
            os.path.join("include/text_storage", file_name) for file_name in file_names
        ]
        return file_paths

    @task_group(group_id="product_identification")
    def product_identification(file_path):

        @task
        def product_identifier(file):
            from openai import OpenAI

            with open(file, "r") as f:
                text_file = f.read()

            prompt = f"""
                Identify the Apache Airflow feature mentioned in the following text.
                The options are: [Datasets, Dynamic Task Mapping, NOT SPECIFIED].
                Text: {text_file} .
                Only return the feature name name.
                """

            client = OpenAI()

            chat_completion = client.chat.completions.create(
                model="gpt-4", messages=[{"role": "user", "content": prompt}]
            )

            return {"file": file, "product": chat_completion.choices[0].message.content}

        product_info = product_identifier(file_path)

        return product_info

    @task 
    def save_tags(tags):
        tags = {tag["file"]: tag["product"] for tag in tags}

        import json
        with open("include/tags.json", "w") as f:
            json.dump(tags, f)

        

    high_quality_texts = get_high_quality_texts()

    product_identification_tg = product_identification.expand(
        file_path=high_quality_texts
    )

    save_tags(product_identification_tg)



    chain(
        start,
        high_quality_texts,
        product_identification_tg,
        end,
    )


tag_feedback_with_product()
