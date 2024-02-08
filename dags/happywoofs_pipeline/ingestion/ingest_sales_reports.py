"""
## Dynamically generated DAG for ingestion of sales_reports

This DAG is generated dynamically from the `ingestion_source_config.json` file.
"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.sensors.base import PokeReturnValue
from airflow.io.path import ObjectStoragePath
from airflow.operators.empty import EmptyOperator
from pendulum import duration, parse

from include.dynamic_dag_generation.helper_functions import evaluate_new_file, verify_checksum

@dag(
    dag_id="ingest_sales_reports",
    start_date=parse("2024-01-01T00:00:00Z"),
    schedule="@daily",
    catchup=False,
    tags=["sales_reports", "ETL", "HappyWoofs"],
    default_args={
        "owner": "Avery",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    description=f"Ingest data from sales_reports",
    doc_md=__doc__,
)
def ingest_dag():
    @task.sensor(
        task_id=f"wait_for_new_files_sales_reports",
        doc="This task waits for new files to arrive in the source bucket.",
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )
    def wait_for_new_files(
        base_path: ObjectStoragePath, source_name: str, conn_id_ingest: str
    ) -> PokeReturnValue:
        """Wait for a new file to arrive in the source satisfying given criteria."""
        path = ObjectStoragePath(
            f"{base_path}{source_name}", conn_id=conn_id_ingest
        )

        files = [f for f in path.iterdir() if f.is_file()]
        is_condition_met = evaluate_new_file(files)

        return PokeReturnValue(is_done=is_condition_met, xcom_value=files)

    @task(task_id=f"extract_sales_reports")
    def extract(
        base_path_intermediate: ObjectStoragePath,
        conn_id_intermediate: str,
        source_name: str,
        source_file: ObjectStoragePath,
    ) -> list:
        """Extract data from source and write it to intermediary storage."""

        print(f"Extracting {source_file} and copy to {base_path_intermediate}.")

        file_name = source_file.name

        intermediate_file_loc = ObjectStoragePath(
            f"{base_path_intermediate}{source_name}/{file_name}",
            conn_id=conn_id_intermediate,
        )

        source_file.copy(dst=intermediate_file_loc)

        return intermediate_file_loc

    @task(task_id=f"verify_checksum_sales_reports")
    def check_checksum(
        file: ObjectStoragePath,
    ) -> list:
        check_sum_file = file.checksum()

        result = verify_checksum(check_sum_file)

        return result

    @task(task_id=f"load_sales_reports")
    def load(
        source_file: ObjectStoragePath,
        base_path_load: ObjectStoragePath,
        conn_id_load: str,
    ) -> list:
        "Load data from intermediary to load storage."
        print(f"Extracting {source_file} and writing it to {base_path_load}.")

        file_name = source_file.name

        load_file_loc = ObjectStoragePath(
            f"{base_path_load}sales_reports/{file_name}",
            conn_id=conn_id_load,
        )

        source_file.copy(dst=load_file_loc)

        return load_file_loc

    update_dataset_obj = EmptyOperator(
        task_id=f"update_dataset_sales_reports",
        outlets=[Dataset("s3://ce-2-8-examples-bucket/load/sales_reports")]
    )

    # Calling TaskFlow tasks, inferring dependencies
    source_files = wait_for_new_files(
        "s3://ce-2-8-examples-bucket/ingest/",
        "sales_reports",
        "aws_de_team"
    )

    # Dynamically map the extract, transform and load tasks over the list of new file locations
    extract_obj = extract.partial(
        base_path_intermediate="s3://ce-2-8-examples-bucket/process/",
        conn_id_intermediate="aws_de_team", 
        source_name="sales_reports",
    ).expand(source_file=source_files)

    checked_files = check_checksum.expand(file=extract_obj)

    load_obj = load.partial(
        base_path_load="s3://ce-2-8-examples-bucket/load/",
        conn_id_load="aws_de_team",
    ).expand(source_file=extract_obj)

    # Define dependencies explicitly
    chain(checked_files, load_obj, update_dataset_obj)

ingest_dag()
