from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime, now

from include.functions import source_unit, extract, load, transform

_SRC_PARAM_PREFIX = "source_"

sources = [
    {
        "name": "local_files",
        "source_parameters": {
            "source_unit_function": source_unit.get_local_files_source_units,
            "kwargs": {"uri": "file://include/data"},
        },
        "extract_parameters": {
            "extraction_function": extract.extract_local_files,
            "kwargs": {},
        },
    },
    {
        "name": "gh_issues", 
        "source_parameters": {
            "source_unit_function": source_unit.get_gh_repos,
            "kwargs": {"list_of_repos": ["apache/airflow"]},
        }, 
        "extract_parameters": {
            "extraction_function": extract.extract_issues_gh_repo,
            "kwargs": {},
        },
    }
]

source_params = {
    f'{_SRC_PARAM_PREFIX}{src["name"]}': Param(
        False, type="boolean", description="Toggle on for ingestion!"
    )
    for src in sources
}


@dag(
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    params={
        "ingest_start_date": Param(
            "2024-06-01T00:00:00+00:00", type="string", format="date-time"
        ),
        "ingest_end_time": Param(now().timestamp(), type="string", format="date-time"),
        **source_params,
    },
)
def ingest_product_feedback():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed", outlets=[Dataset("data_ingested")])

    @task.branch
    def pick_ingestion_sources(**context):
        list_of_all_src_params = [
            src for src in context["params"] if src.startswith(_SRC_PARAM_PREFIX)
        ]
        list_of_true_src_params = [
            src for src in list_of_all_src_params if context["params"][src]
        ]

        task_ids_to_run = [
            f"fetch_{src_name.replace(_SRC_PARAM_PREFIX, '')}_source_units"
            for src_name in list_of_true_src_params
        ]

        return task_ids_to_run

    pick_ingestion_sources_obj = pick_ingestion_sources()

    for source in sources:

        source_units = task(
            source["source_parameters"]["source_unit_function"],
            task_id=f"fetch_{source['name']}_source_units",
        )(**source["source_parameters"]["kwargs"])

        @task_group(
            group_id=f"ingest_{source['name']}",
        )
        def ingestion(source_unit):

            texts = task(
                source["extract_parameters"]["extraction_function"],
                task_id=f"extract_{source['name']}",
            )(source_unit=source_unit)

            transformed_texts = task(
                transform.transform_text,
                task_id=f"transform_{source['name']}",
            )(texts)

            task(
                load.load_text_chunks_to_object_storage,
                task_id=f"load_{source['name']}_chunks",
            )(transformed_texts)

        ingestion_tg = ingestion.expand(source_unit=source_units)

        chain(start, pick_ingestion_sources_obj, source_units, ingestion_tg, end)


ingest_product_feedback()
