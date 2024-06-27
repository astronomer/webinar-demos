from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from pendulum import datetime
import json


@dag(
    start_date=datetime(2024, 6, 1),
    schedule=[Dataset("data_tagged_with_product")],
    catchup=False,
)
def aggregate_feedback_per_product():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("data_summarized")])

    @task
    def get_file_list_per_product():

        with open("include/tags.json", "r") as f:
            config = json.load(f)

            inverted_data = {}
            for key, value in config.items():
                if value not in inverted_data:
                    inverted_data[value] = [key]
                else:
                    inverted_data[value].append(key)

            with open("include/files_per_product.json", "w") as f:
                json.dump(inverted_data, f)

        return inverted_data

    @task
    def get_list_of_products():
        with open("include/files_per_product.json", "r") as f:
            config = json.load(f)
            products = list(config.keys())

        return products

    @task
    def aggregate_feedback_per_product(product):
        with open("include/files_per_product.json", "r") as f:
            config = json.load(f)
            files = config[product]

        feedback = []
        for file in files:
            with open(file, "r") as f:
                feedback.append(f.read())

        return {"product": product, "feedback": feedback}

    @task
    def summarize_feedback(input):
        from openai import OpenAI
        from include.functions.utils.token_wrangling import count_tokens, split_text_to_fit_tokens

        feedback = " ".join(input["feedback"])
        product = input["product"]

        max_tokens = 5000  # Define the max tokens per API call, adjust as needed

        prompt_template = """
        Summarize the feedback for the Apache Airflow feature {product}.
        Also score the general sentiment of the feedback.
        Text: {feedback} .
        Respond in the format: "The feedback for {product} is: INSERT SUMMARY. The sentiment score is: INSERT SCORE".
        """

        total_tokens = count_tokens(
            prompt_template.format(product=product, feedback=feedback)
        )

        if total_tokens > max_tokens:
            feedback_chunks = split_text_to_fit_tokens(
                feedback,
                max_tokens
                - count_tokens(prompt_template.format(product=product, feedback="")),
            )
        else:
            feedback_chunks = [feedback]

        summaries = []
        client = OpenAI() 

        for chunk in feedback_chunks:
            prompt = prompt_template.format(product=product, feedback=chunk)
            chat_completion = client.chat.completions.create(
                model="gpt-4", messages=[{"role": "user", "content": prompt}]
            )
            summaries.append(chat_completion.choices[0].message.content.strip())

        final_summary_prompt = f"""
        Combine the following summaries into one coherent summary for the product {product}:
        {' '.join(summaries)}
        """
        final_summary_completion = client.chat.completions.create(
            model="gpt-4", messages=[{"role": "user", "content": final_summary_prompt}]
        )

        final_summary = final_summary_completion.choices[0].message.content.strip()

        return {"product": product, "summary": final_summary}

    @task
    def write_summary(input):
        product = input["product"]
        summary = input["summary"]
        with open(f"include/feedback/{product}.txt", "w") as f:
            f.write(summary)

    list_per_product = get_file_list_per_product()
    products = get_list_of_products()
    agg_feedback = aggregate_feedback_per_product.expand(product=products)
    summaries = summarize_feedback.expand(input=agg_feedback)
    write_summaries = write_summary.expand(input=summaries)

    chain(
        start, list_per_product, products, agg_feedback, summaries, write_summaries, end
    )


aggregate_feedback_per_product()
