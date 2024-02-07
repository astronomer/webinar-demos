"""
## Analyzes our customer feedback 

Our customers only deserve the best toys!
Remember these feedback comments are only a proxy since
they were submitted by our customer's humans.

![A very good dog](https://place.dog/300/200)
"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task_group, task
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
import string
import os


SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_de_team")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("snowflake://customer_feedback_table")],
    catchup=False,
    tags=["ML"],
    default_args={"owner": "Pakkun", "retries": 3, "retry_delay": 5},
    description="Analyze customer feedback",
    doc_md=__doc__,
)
def analyze_customer_feedback():
    gather_feedback = SnowflakeOperator(
        task_id=f"gather_feedback",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT DISTINCT(COMMENTS) FROM customer_feedback_table;
        """,
    )

    @task_group
    def get_sentiment(feedback):
        @task
        def process_feedback(feedback):
            comment = feedback["COMMENTS"].translate(
                str.maketrans("", "", string.punctuation)
            )

            return comment

        @task(
            queue="ml-queue",
        )
        def analyze_sentiment(processed_text):
            from transformers import pipeline

            sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            )
            sentence = processed_text
            result = sentiment_pipeline(sentence)
            print(result)
            return result

        return analyze_sentiment(process_feedback(feedback))

    @task
    def report_on_results(sentiments):
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        for sentiment in sentiments:
            label = sentiment[0]["label"]
            if label == "positive":
                positive_count += 1
            elif label == "negative":
                negative_count += 1
            else:
                neutral_count += 1

        print(
            f"Positive: {positive_count}, Negative: {negative_count}, Neutral: {neutral_count}"
        )
        return positive_count, negative_count, neutral_count

    sentiments = get_sentiment.expand(feedback=gather_feedback.output)
    report = report_on_results(sentiments)

    chain(sentiments, report)


analyze_customer_feedback()
