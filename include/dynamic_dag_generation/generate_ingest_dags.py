"""
Generates ingestion DAGs based on the configuration file ingestion_source_config.json.
"""

import json
import os
import shutil
import fileinput

config_filepath = f"{os.path.dirname(__file__)}/ingestion_source_config.json"
dag_template_filename = f"{os.path.dirname(__file__)}/ingest_dag_template.py"

with open(f"{os.path.dirname(__file__)}/ingestion_source_config.json", "r") as f:
    config = json.load(f)

    for source in config["sources"]:
        source_name = source["source_name"]
        base_path_ingest = source["base_path_ingest"]
        conn_id_ingest = source["conn_id_ingest"]
        base_path_intermediate = source["base_path_intermediate"]
        conn_id_intermediate = source["conn_id_intermediate"]
        base_path_load = source["base_path_load"]
        conn_id_load = source["conn_id_load"]
        dataset_uri = source["dataset_uri"]
        dag_id = source["dag_id"]
        start_date = source["start_date"]
        schedule = source["schedule"]
        catchup = source["catchup"]
        tags = source["tags"]
        dag_owner = source["dag_owner"]
        task_retries = source["task_retries"]

        new_filename = "dags/happywoofs_pipeline/ingestion/" + dag_id + ".py"
        shutil.copyfile(dag_template_filename, new_filename)

        for line in fileinput.input(new_filename, inplace=True):
            line = line.replace("dag_id_to_replace", '"' + dag_id + '"')
            line = line.replace("start_date_to_replace", '"' + str(start_date) + '"')
            line = line.replace("schedule_to_replace", '"' + schedule + '"')
            line = line.replace("catchup_to_replace", str(catchup))
            line = line.replace("tags_to_replace", json.dumps(tags))
            line = line.replace("dag_owner_to_replace", '"' + dag_owner + '"')
            line = line.replace("task_retries_to_replace", str(task_retries))
            line = line.replace("source_name_to_replace", source_name)
            line = line.replace("base_path_ingest_to_replace", '"' + str(base_path_ingest)+ '"')
            line = line.replace("conn_id_ingest_to_replace", '"' + conn_id_ingest + '"')
            line = line.replace("base_path_intermediate_to_replace", '"' + str(base_path_intermediate) + '"')
            line = line.replace("conn_id_intermediate_to_replace", '"' + conn_id_intermediate + '"')
            line = line.replace("base_path_load_to_replace", '"' + base_path_load + '"')
            line = line.replace("conn_id_load_to_replace", '"' + conn_id_load + '"')
            line = line.replace("dataset_uri_to_replace", '"' + dataset_uri + '"')
            line = line.replace("path_extension_to_replace", '"' + source_name + '"')

            print(line, end="")