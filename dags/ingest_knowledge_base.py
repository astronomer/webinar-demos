"""
## Ingest the knowledge base data into the vector database
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime
from functools import reduce

from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.task_functions import extract, split, prep_for_ingest


# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_default"
KNOWLEDGE_BASE_DATASET_URIS = [
    "file://include/knowledge_base/guides",
    "file://include/knowledge_base/text_files",
]

# Provide the class name you want to ingest the data into.
WEAVIATE_CLASS_NAME = "KB"
# set the vectorizer to text2vec-openai if you want to use the openai model
# note that using the OpenAI vectorizer requires a valid API key in the
# AIRFLOW_CONN_WEAVIATE_DEFAULT connection.
# If you want to use a different vectorizer
# (https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules)
# make sure to also add it to the weaviate configuration's `ENABLE_MODULES` list
# for example in the docker-compose.override.yml file
VECTORIZER = "text2vec-openai"
SCHEMA_JSON_PATH = "include/weaviate/schema.json"

document_sources = [
    {
        "name": "guides",
        "extract_parameters": {
            "extract_function": extract.extract_guides,
            "folder_path": "include/knowledge_base/guides/",
        },
    },
    {
        "name": "text_files",
        "extract_parameters": {
            "extract_function": extract.extract_text_files,
            "folder_path": "include/knowledge_base/text_files/",
        },
    },
]


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=reduce(lambda x, y: Dataset(x) | Dataset(y), KNOWLEDGE_BASE_DATASET_URIS),
    ),
    catchup=False,
    tags=["ingest"],
    default_args={
        "retries": 0,
        "owner": "Astronomer",
    },
)
def ingest_knowledge_base():

    @task.branch
    def check_class(conn_id: str, class_name: str) -> bool:
        "Check if the provided class already exists and decide on the next step."
        hook = WeaviateHook(conn_id)
        client = hook.get_client()

        if not client.schema.get()["classes"]:
            print("No classes found in this weaviate instance.")
            return "create_class"

        existing_classes_names_with_vectorizer = [
            x["class"] for x in client.schema.get()["classes"]
        ]

        if class_name in existing_classes_names_with_vectorizer:
            print(f"Schema for class {class_name} exists.")
            return "class_already_exists"
        else:
            print(f"Class {class_name} does not exist yet.")
            return "create_class"

    @task
    def create_class(
        conn_id: str, class_name: str, vectorizer: str, schema_json_path: str
    ):
        "Create a class with the provided name, schema and vectorizer."
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

    chain(
        check_class(conn_id=WEAVIATE_CONN_ID, class_name=WEAVIATE_CLASS_NAME),
        [
            schema_already_exists,
            create_class(
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                vectorizer=VECTORIZER,
                schema_json_path=SCHEMA_JSON_PATH,
            ),
        ],
        ingest_document_sources,
    )

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
            )
            .partial(
                class_name=WEAVIATE_CLASS_NAME,
            )
            .expand(record=split_texts)
        )

        import_data = WeaviateIngestOperator.partial(
            task_id=f"import_data_{document_source['name']}",
            map_index_template="{{ task.input_data[0]['uri'] }} - Chunk: {{ task.input_data[0]['chunk_index'] }} / {{ task.input_data[0]['chunks_per_doc'] }}",
            conn_id=WEAVIATE_CONN_ID,
            class_name=WEAVIATE_CLASS_NAME,
            retries=3,
            retry_delay=30,
            trigger_rule="all_done",
        ).expand(input_data=embed_obj)

        chain(ingest_document_sources, texts, split_texts, embed_obj, import_data)


ingest_knowledge_base()
