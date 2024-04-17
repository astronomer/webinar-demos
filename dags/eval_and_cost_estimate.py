"""
## 

"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.param import Param
from airflow.models.baseoperator import chain
from pendulum import datetime
import logging
import os

TOKEN_ENCODING = "cl100k_base"
t_log = logging.getLogger("airflow.task")

STOP_TASK_ID = "over_budget_stop"
CONTINUE_TASK_ID = "in_budget_continue"
TRAIN_EXAMPLES_DIR = "include/examples/train_examples/formatted_examples/"
VALIDATION_EXAMPLES_DIR = "include/examples/validation_examples/formatted_examples/"


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=(
        Dataset(f"file://{TRAIN_EXAMPLES_DIR}")
        | Dataset(f"file://{VALIDATION_EXAMPLES_DIR}")
    ),
    catchup=False,
    tags=["data_quality", "cost_control"],
    default_args={
        "retries": 0,
        "owner": "Astronomer",
    },
    params={
        "max_cost_allowed": Param(
            20,
            type="integer",
            description="The maximum cost allowed for fine-tuning.",
        ),
        "token_encoding": Param(
            "cl100k_base",
            type="string",
        ),
        "fine_tune_per_m_price": Param(
            8,
            type="integer",
        ),
        "num_epochs": Param(
            10,
            type="integer",
        ),
    },
)
def eval_and_cost_estimate():

    @task
    def get_train_examples_file_paths(directory):
        """
        Get a list of file paths in a directory.

        Args:
            directory (str): Path to the directory containing files.
        """

        file_paths = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]
        return file_paths

    @task
    def get_validation_examples_file_paths(directory):
        """
        Get a list of file paths in a directory.

        Args:
            directory (str): Path to the directory containing files.
        """

        file_paths = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]
        return file_paths

    @task(
        map_index_template="{{ custom_map_index }}",
    )
    def check_examples_valid_formatting(example_file_path):
        from airflow.operators.python import get_current_context
        import json
        from include.task_functions.examples_validation import validate_example_format

        with open(example_file_path, "r", encoding="utf-8") as f:
            dataset = [json.loads(line) for line in f]

        num_examples = len(dataset)

        format_errors = validate_example_format(dataset)

        print("First example, content:")
        for message in dataset[0]["messages"]:
            print(message)

        context = get_current_context()
        context["custom_map_index"] = (
            f"Parsing {num_examples} examples from: {example_file_path}"
        )

        return format_errors

    @task(
        map_index_template="{{ custom_map_index }}",
    )
    def count_tokens(example_file_path):
        import json
        from airflow.operators.python import get_current_context
        from include.task_functions.token_counting import dataset_token_analysis

        context = get_current_context()

        with open(example_file_path, "r", encoding="utf-8") as f:
            dataset = [json.loads(line) for line in f]

        encoding = context["params"]["token_encoding"]
        n_billing_tokens_in_dataset = dataset_token_analysis(dataset, encoding)

        context["custom_map_index"] = (
            f"{example_file_path} has {n_billing_tokens_in_dataset} billing tokens."
        )

        return n_billing_tokens_in_dataset

    @task
    def calculate_expected_cost(list_of_num_billing_tokens, **context):
        fine_tune_per_m_price = context["params"]["fine_tune_per_m_price"]
        num_epochs = context["params"]["num_epochs"]
        n_billing_tokens = sum(list_of_num_billing_tokens)

        total_cost = fine_tune_per_m_price * num_epochs * n_billing_tokens / 1e6

        t_log.info(
            f"Expected fine-tuning cost: ${total_cost} for {n_billing_tokens}"
            f" tokens training for {num_epochs} epochs."
        )
        return total_cost

    @task.branch
    def decide_if_cost_within_budget(
        total_cost, stop_task_id, continue_task_id, **context
    ):
        max_cost_allowed = context["params"]["max_cost_allowed"]
        if total_cost <= max_cost_allowed:
            return continue_task_id
        else:
            return stop_task_id

    @task(
        task_id=CONTINUE_TASK_ID,
    )
    def continue_task():
        t_log.info("Cost within budget. Starting fine-tuning DAG.")

    @task(
        task_id=STOP_TASK_ID,
        on_success_callback=lambda x: x.log.info(
            "Task stopped."
        ),  # TODO: Add Slack notification
    )
    def stop_task():
        t_log.info("Cost exceeds budget. Stopping pipeline.")

    @task
    def create_fine_tuning_example_file(example_file_paths, **context):
        """
        Combines all example files into a single file for fine-tuning.
        Args:
            example_file_paths (list): List of file paths to example files.
        Returns:
            str: Path to the combined example file.
        """

        ts = context["ts_nodash"]

        example_type = example_file_paths[0].split("/")[-3]

        combined_fine_tune_examples_file_path = f"include/examples/{example_type}/fine_tune_examples/{ts}_combined_examples.jsonl"

        os.makedirs(
            os.path.dirname(combined_fine_tune_examples_file_path), exist_ok=True
        )

        with open(combined_fine_tune_examples_file_path, "w", encoding="utf-8") as f:
            for file_path in example_file_paths:
                with open(file_path, "r", encoding="utf-8") as example_file:
                    for line in example_file:
                        f.write(line)

        return combined_fine_tune_examples_file_path

    # ---------------------------------- #
    # Call tasks and define Dependencies #
    # ---------------------------------- #

    get_train_examples_file_paths_obj = get_train_examples_file_paths(
        TRAIN_EXAMPLES_DIR
    )
    get_validation_examples_file_paths_obj = get_validation_examples_file_paths(
        VALIDATION_EXAMPLES_DIR
    )

    check_train_examples_valid_formatting_obj = (
        check_examples_valid_formatting.override(
            task_display_name="Check formatting train examples"
        ).expand(example_file_path=get_train_examples_file_paths_obj)
    )

    check_validation_examples_valid_formatting_obj = (
        check_examples_valid_formatting.override(
            task_display_name="Check formatting validation examples",
        ).expand(example_file_path=get_validation_examples_file_paths_obj)
    )

    count_tokens_obj = count_tokens.expand(
        example_file_path=get_train_examples_file_paths_obj
    )

    calculate_expected_cost_obj = calculate_expected_cost(count_tokens_obj)

    decide_if_cost_within_budget_obj = decide_if_cost_within_budget(
        total_cost=calculate_expected_cost_obj,
        stop_task_id=STOP_TASK_ID,
        continue_task_id=CONTINUE_TASK_ID,
    )

    continue_task_obj = continue_task()
    create_fine_tuning_train_example_file_obj = (
        create_fine_tuning_example_file.override(
            task_id="create_fine_tuning_train_example_file",
            outlets=[
                Dataset(
                    "file://include/examples/train_examples/fine_tune_examples/",
                )
            ],
        )(example_file_paths=get_train_examples_file_paths_obj)
    )
    create_fine_tuning_validation_example_file_obj = (
        create_fine_tuning_example_file.override(
            task_id="create_fine_tuning_validation_example_file",
            outlets=[
                Dataset(
                    "file://include/examples/validation_examples/fine_tune_examples/",
                )
            ],
        )(example_file_paths=get_validation_examples_file_paths_obj)
    )

    chain(check_train_examples_valid_formatting_obj, count_tokens_obj)
    chain(decide_if_cost_within_budget_obj, [stop_task(), continue_task_obj])
    chain(continue_task_obj, create_fine_tuning_train_example_file_obj)
    chain(
        check_validation_examples_valid_formatting_obj,
        create_fine_tuning_validation_example_file_obj,
    )


eval_and_cost_estimate()
