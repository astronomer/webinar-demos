"""
## Create a schema in Weaviate

CAUTION: This DAG will delete all schemas in Weaviate in your Weaviate instance. Please use it with caution.
"""

from datetime import datetime
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.decorators import dag, task
import os

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID", "weaviate_default")
# Provide the class name to delete the schema.
WEAVIATE_CLASS_TO_DELETE = os.getenv("DEFAULT_WEAVIATE_CLASS_NAME")


default_args = {
    "retries": 0,
    "owner": "DE Team",
}


@dag(
    dag_display_name="🧼 Clear Weaviate",
    schedule=None,
    start_date=datetime(2023, 10, 18),
    catchup=False,
    default_args=default_args,
    tags=["helper", "use-case"],
)
def clear_weaviate():

    @task
    def delete_all_weaviate_schemas(class_to_delete=None):
        WeaviateHook(WEAVIATE_CONN_ID).get_conn().schema.delete_class(class_to_delete)

    delete_all_weaviate_schemas(class_to_delete=WEAVIATE_CLASS_TO_DELETE)


clear_weaviate()
