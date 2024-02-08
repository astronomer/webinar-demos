"""
This file shows an example of single-file dynamic DAG generation. 
Remove the dags from /dags/happywoofs_pipeline/ingestion/ and then place this DAG (uncommented) 
anywhere in the /dags/ directory to see the dynamic DAG generation in action.

Note that this way of generating DAGs works well for a small number of DAGs
but can lead to performance issues when generating a large number of DAGs.
"""

# """
# ## Dynamically generated DAGs for ingestion

# This DAG is generated dynamically from the `ingestion_source_config.json` file.
# There is one DAG per source.
# """

# from airflow.datasets import Dataset
# from airflow.decorators import dag, task
# from airflow.models.baseoperator import chain
# from airflow.sensors.base import PokeReturnValue
# from airflow.io.path import ObjectStoragePath
# from airflow.operators.empty import EmptyOperator
# from pendulum import parse, datetime, duration
# from typing import Any
# import json
# from include.dynamic_dag_generation.helper_functions import evaluate_new_file, verify_checksum


# def create_ingest_dags(
#     source_name: str,
#     base_path_ingest: ObjectStoragePath,
#     conn_id_ingest: str,
#     base_path_intermediate: ObjectStoragePath,
#     conn_id_intermediate: str,
#     base_path_load: ObjectStoragePath,
#     conn_id_load: str,
#     dataset_uri: str,
#     dag_id: str,
#     start_date: datetime = None,
#     schedule: Any = None,
#     catchup: bool = False,
#     tags: list = ["NONE"],
#     dag_owner: str = "airflow",
#     task_retries: int = 3,
#     task_retry_delay: duration = duration(minutes=5),
# ):
#     @dag(
#         dag_id=dag_id,
#         start_date=start_date,
#         schedule=schedule,
#         catchup=catchup,
#         tags=tags,
#         default_args={
#             "owner": dag_owner,
#             "retries": task_retries,
#             "retry_delay": task_retry_delay,
#         },
#         description=f"Ingest data from {source_name}",
#         doc_md=__doc__,
#     )
#     def ingest_dag():
#         @task.sensor(
#             task_id=f"wait_for_new_files_{source_name}",
#             doc="This task waits for new files to arrive in the source bucket.",
#             poke_interval=30,
#             timeout=3600,
#             mode="poke",
#         )
#         def wait_for_new_files(
#             base_path: ObjectStoragePath, source_name: str, conn_id_ingest: str
#         ) -> PokeReturnValue:
#             """Wait for a new file to arrive in the source satisfying given criteria."""
#             path = ObjectStoragePath(
#                 f"{base_path}{source_name}", conn_id=conn_id_ingest
#             )

#             files = [f for f in path.iterdir() if f.is_file()]
#             is_condition_met = evaluate_new_file(files)

#             return PokeReturnValue(is_done=is_condition_met, xcom_value=files)

#         @task(task_id=f"extract_{source_name}")
#         def extract(
#             base_path_intermediate: ObjectStoragePath,
#             conn_id_intermediate: str,
#             source_name: str,
#             source_file: ObjectStoragePath,
#         ) -> list:
#             """Extract data from source and write it to intermediary storage."""

#             print(f"Extracting {source_file} and copy to {base_path_intermediate}.")

#             file_name = source_file.name

#             intermediate_file_loc = ObjectStoragePath(
#                 f"{base_path_intermediate}{source_name}/{file_name}",
#                 conn_id=conn_id_intermediate,
#             )

#             source_file.copy(dst=intermediate_file_loc)

#             return intermediate_file_loc

#         @task(task_id=f"verify_checksum_{source_name}")
#         def check_checksum(
#             file: ObjectStoragePath,
#         ) -> list:
#             check_sum_file = file.checksum()

#             result = verify_checksum(check_sum_file)

#             return result

#         @task(task_id=f"load_{source_name}")
#         def load(
#             source_file: ObjectStoragePath,
#             base_path_load: ObjectStoragePath,
#             conn_id_load: str,
#         ) -> list:
#             "Load data from intermediary to load storage."
#             print(f"Extracting {source_file} and writing it to {base_path_load}.")

#             file_name = source_file.name

#             load_file_loc = ObjectStoragePath(
#                 f"{base_path_load}{source_name}/{file_name}",
#                 conn_id=conn_id_load,
#             )

#             source_file.copy(dst=load_file_loc)

#             return load_file_loc

#         update_dataset_obj = EmptyOperator(
#             task_id=f"update_dataset_{source_name}", outlets=[Dataset(dataset_uri)]
#         )

#         # Calling TaskFlow tasks, inferring dependencies
#         source_files = wait_for_new_files(base_path_ingest, source_name, conn_id_ingest)

#         # Dynamically map the extract, transform and load tasks over the list of new file locations
#         extract_obj = extract.partial(
#             base_path_intermediate=base_path_intermediate,
#             conn_id_intermediate=conn_id_intermediate,
#             source_name=source_name,
#         ).expand(source_file=source_files)

#         checked_files = check_checksum.expand(file=extract_obj)

#         load_obj = load.partial(
#             base_path_load=base_path_load,
#             conn_id_load=conn_id_load,
#         ).expand(source_file=extract_obj)

#         # Define dependencies explicitly
#         chain(checked_files, load_obj, update_dataset_obj)

#     ingest_dag_obj = ingest_dag()

#     return ingest_dag_obj


# with open("include/dynamic_dag_generation/ingestion_source_config.json", "r") as f:
#     config = json.load(f)


# for source in config["sources"]:
#     source_name = source["source_name"]
#     base_path_ingest = ObjectStoragePath(source["base_path_ingest"])
#     conn_id_ingest = source["conn_id_ingest"]
#     base_path_intermediate = ObjectStoragePath(source["base_path_intermediate"])
#     conn_id_intermediate = source["conn_id_intermediate"]
#     base_path_load = source["base_path_load"]
#     conn_id_load = source["conn_id_load"]
#     dataset_uri = source["dataset_uri"]
#     dag_id = source["dag_id"]
#     start_date = parse(source["start_date"])
#     schedule = source["schedule"]
#     catchup = source["catchup"]
#     tags = source["tags"]
#     dag_owner = source["dag_owner"]
#     task_retries = source["task_retries"]

#     globals()[dag_id] = create_ingest_dags(
#         source_name=source_name,
#         base_path_ingest=base_path_ingest,
#         conn_id_ingest=conn_id_ingest,
#         base_path_intermediate=base_path_intermediate,
#         conn_id_intermediate=conn_id_intermediate,
#         base_path_load=base_path_load,
#         conn_id_load=conn_id_load,
#         dataset_uri=dataset_uri,
#         dag_id=dag_id,
#         start_date=start_date,
#         schedule=schedule,
#         catchup=catchup,
#         tags=tags,
#         dag_owner=dag_owner,
#         task_retries=task_retries,
#     )
