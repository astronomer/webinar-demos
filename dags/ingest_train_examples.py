"""
## Ingest examples for model fine-tuning

"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
import os


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=(
        Dataset("file://include/examples/train_examples/examples_long")
        | Dataset("file://include/examples/train_examples/examples_short")
    ),
    catchup=False,
    tags=["ingest"],
    default_args={
        "retries": 0,
        "owner": "Astronomer",
    },
)
def ingest_train_examples():

    @task
    def get_example_folders(directory):
        """
        Get a list of folders in a directory.

        Args:
            directory (str): Path to the directory containing folders.
        """

        folders = [
            f
            for f in os.listdir(directory)
            if os.path.isdir(os.path.join(directory, f))
        ]
        return folders

    @task(
        map_index_template="{{ custom_map_index }}",
    )
    def create_jsonl_from_txt_examples(
        example_folder, output_path="include/examples/train_examples/formatted_examples/"
    ):
        """
        Convert text files in a directory to a JSON Lines file.

        Args:
            directory (str): Path to the directory containing text files.
            output_file (str): Path to the output JSONL file.
        """
        import json
        from airflow.operators.python import get_current_context

        jsonl_data = []

        directory = os.path.join("include/examples/train_examples/", example_folder)

        for filename in os.listdir(directory):
            if directory != "formatted_examples":
                if filename.endswith(".txt"):
                    file_path = os.path.join(directory, filename)

                    with open(file_path, "r", encoding="utf-8") as file:
                        lines = file.readlines()
                        title = lines[0].strip().replace("## ", "")
                        content = "".join(lines[1:]).strip()

                        formatted_dict = {
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "Astra is a kind and helpful bot creating highly technical posts about Apache Airflow.",
                                },
                                {
                                    "role": "user",
                                    "content": f"Create a post about: {title}",
                                },
                                {"role": "assistant", "content": content},
                            ]
                        }

                        jsonl_data.append(formatted_dict)

        directory_name = os.path.basename(directory)

        output_file = os.path.join(output_path, f"{directory_name}.jsonl")

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        with open(output_file, "w", encoding="utf-8") as outfile:
            for entry in jsonl_data:
                json.dump(entry, outfile)
                outfile.write("\n")

        print(f"JSON Lines file '{output_file}' created successfully.")

        context = get_current_context()
        context["custom_map_index"] = (
            f"Parsing examples from: include/examples/train_examples/{directory_name}"
        )

    @task(outlets=[Dataset("file://include/examples/train_examples/formatted_examples/")])
    def examples_ingested():
        print("Examples ingested successfully.")

    chain(
        create_jsonl_from_txt_examples.expand(
            example_folder=get_example_folders("include/examples/train_examples/")
        ),
        examples_ingested(),
    )


ingest_train_examples()
