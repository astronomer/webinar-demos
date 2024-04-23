"""
## Ingest the knowledge base data into the vector database

This DAG chunks the text data from the knowledge base into smaller pieces 
and ingests it into the Weaviate vector database.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from pendulum import datetime, duration
from functools import reduce
import os

from include.task_functions import extract, split, prep_for_ingest

# Variables used in the DAG
_KNOWLEDGE_BASE_DATA_GUIDES_URI = os.getenv("KNOWLEDGE_BASE_DATA_GUIDES_URI")
_KNOWLEDGE_BASE_DATA_TEXT_FILES_URI = os.getenv("KNOWLEDGE_BASE_DATA_TEXT_FILES_URI")
_KNOWLEDGE_BASE_DATASET_URIS = [
    _KNOWLEDGE_BASE_DATA_GUIDES_URI,
    _KNOWLEDGE_BASE_DATA_TEXT_FILES_URI,
]
_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_DEFAULT_WEAVIATE_CLASS_NAME = os.getenv("DEFAULT_WEAVIATE_CLASS_NAME")
_DEFAULT_WEAVIATE_VECTORIZER = os.getenv("DEFAULT_WEAVIATE_VECTORIZER")
_WEAVIATE_SCHEMA_PATH = os.getenv("WEAVIATE_SCHEMA_PATH")

_CREATE_CLASS_TASK_ID = "create_class"
_CLASS_ALREADY_EXISTS_TASK_ID = "class_already_exists"

document_sources = [
    {
        "name": "guides",
        "extract_parameters": {
            "extract_function": extract.extract_guides,
            "folder_path": _KNOWLEDGE_BASE_DATA_GUIDES_URI.split("://")[1],
        },
    },
    {
        "name": "text_files",
        "extract_parameters": {
            "extract_function": extract.extract_text_files,
            "folder_path": _KNOWLEDGE_BASE_DATA_TEXT_FILES_URI.split("://")[1],
        },
    },
]


@dag(
    dag_display_name="📚 Ingest Knowledge Base",
    start_date=datetime(2024, 4, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=reduce(
            lambda x, y: Dataset(x) | Dataset(y), _KNOWLEDGE_BASE_DATASET_URIS
        ),
    ),
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["ingest", "use-case"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "owner": "DE Team",
    },
    doc_md=__doc__,
    description="Ingest knowledge base into the vector database.",
)
def ingest_knowledge_base():

    @task.branch
    def check_class(
        conn_id: str,
        class_name: str,
        create_class_task_id: str,
        class_already_exists_task_id: str,
    ) -> str:
        """
        Check if the class exists in the Weaviate schema.
        Args:
            conn_id: The connection ID to use.
            class_name: The name of the class to check.
            create_class_task_id: The task ID to execute if the class does not exist.
            class_already_exists_task_id: The task ID to execute if the class already exists.
        Returns:
            str: Task ID of the next task to execute.
        """
        hook = WeaviateHook(conn_id)
        client = hook.get_client()

        if not client.schema.get()["classes"]:
            print("No classes found in this weaviate instance.")
            return create_class_task_id

        existing_classes_names_with_vectorizer = [
            x["class"] for x in client.schema.get()["classes"]
        ]

        if class_name in existing_classes_names_with_vectorizer:
            print(f"Schema for class {class_name} exists.")
            return class_already_exists_task_id
        else:
            print(f"Class {class_name} does not exist yet.")
            return create_class_task_id

    @task
    def create_class(
        conn_id: str, class_name: str, vectorizer: str, schema_json_path: str
    ) -> None:
        """
        Create a class in the Weaviate schema.
        Args:
            conn_id: The connection ID to use.
            class_name: The name of the class to create.
            vectorizer: The vectorizer to use for the class.
            schema_json_path: The path to the schema JSON file.
        """
        import json

        weaviate_hook = WeaviateHook(conn_id)

        with open(schema_json_path) as f:
            schema = json.load(f)
            class_obj = next(
                (item for item in schema["classes"] if item["class"] == class_name),
                None,
            )
            class_obj["vectorizer"] = vectorizer

        weaviate_hook.create_class(class_obj)

    schema_already_exists = EmptyOperator(task_id="class_already_exists")

    ingest_document_sources = EmptyOperator(
        task_id="ingest_document_sources", trigger_rule="none_failed"
    )

    # create paths for each document source
    for document_source in document_sources:
        texts = task(
            document_source["extract_parameters"]["extract_function"],
            task_id=f"extract_{document_source['name']}",
        )(document_source["extract_parameters"]["folder_path"])

        split_texts = task(
            split.split_text,
            task_id=f"split_text_{document_source['name']}",
            trigger_rule="all_done",
        )(texts)

        embed_obj = (
            task(
                prep_for_ingest.import_data,
                task_id=f"prep_for_ingest_{document_source['name']}",
                map_index_template="{{ custom_map_index }}",
                multiple_outputs=False,
            )
            .partial(
                class_name=_DEFAULT_WEAVIATE_CLASS_NAME,
            )
            .expand(record=split_texts)
        )

        import_data = WeaviateIngestOperator.partial(
            task_id=f"import_data_{document_source['name']}",
            map_index_template="{{ task.input_data[0]['uri'] }} - Chunk: {{ task.input_data[0]['chunk_index'] }} / {{ task.input_data[0]['chunks_per_doc'] }}",
            conn_id=_WEAVIATE_CONN_ID,
            class_name=_DEFAULT_WEAVIATE_CLASS_NAME,
            retries=3,
            retry_delay=30,
            trigger_rule="all_done",
        ).expand(input_data=embed_obj)

        # set inner loop dependencies
        chain(ingest_document_sources, texts, split_texts, embed_obj, import_data)

    # ---------------------------------- #
    # Call tasks and define dependencies #
    # ---------------------------------- #

    chain(
        check_class(
            conn_id=_WEAVIATE_CONN_ID,
            class_name=_DEFAULT_WEAVIATE_CLASS_NAME,
            create_class_task_id=_CREATE_CLASS_TASK_ID,
            class_already_exists_task_id=_CLASS_ALREADY_EXISTS_TASK_ID,
        ),
        [
            schema_already_exists,
            create_class(
                conn_id=_WEAVIATE_CONN_ID,
                class_name=_DEFAULT_WEAVIATE_CLASS_NAME,
                vectorizer=_DEFAULT_WEAVIATE_VECTORIZER,
                schema_json_path=_WEAVIATE_SCHEMA_PATH,
            ),
        ],
        ingest_document_sources,
    )


ingest_knowledge_base()
