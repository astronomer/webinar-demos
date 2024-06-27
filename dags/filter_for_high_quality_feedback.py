from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from pendulum import datetime
import os

@dag(start_date=datetime(2024, 6, 1), schedule=[Dataset("data_ingested")], catchup=False)
def filter_for_high_quality_feedback():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("data_filtered")])

    @task
    def get_texts():
        file_paths = os.listdir("include/text_storage")
        return file_paths

    @task_group(group_id="filtering")
    def filtering(file_path):

        @task
        def data_quality_filter_1(file):
            # do some data quality filtering
            pass

        @task
        def data_quality_filter_2(file):
            # do some data quality filtering
            pass

        chain(data_quality_filter_1(file_path), data_quality_filter_2(file_path))

    texts = get_texts()

    filtering_tg = filtering.expand(file_path=texts)

    chain(start, texts, filtering_tg, end)

filter_for_high_quality_feedback()
