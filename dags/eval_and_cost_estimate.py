"""
## Evaluate the training and validation examples and estimate the cost of fine-tuning

This DAG makes sure the training and validation examples are correctly formatted 
for fine-tuning. If the format passes, the final train and validation files to be 
used for fine-tuning are created.
It then calculates the expected cost of fine-tuning based on the number of tokens
in the training examples and the cost per million tokens.
The DAG will not create the files for fine-tuning if the cost exceeds the budget,
which stops the dataset dependent pipeline from continuing.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.param import Param
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
import logging
import json
import os

from include.task_functions.examples_validation import validate_example_format
from include.task_functions.token_counting import dataset_token_analysis

# Airflow task logger
t_log = logging.getLogger("airflow.task")

# Variables used in the DAG
_DEFAULT_TOKEN_ENCODING = os.getenv("DEFAULT_TOKEN_ENCODING")
_DEFAULT_MAX_FINE_TUNING_COST = int(os.getenv("DEFAULT_MAX_FINE_TUNING_COST"))
_DEFAULT_FINE_TUNE_PRICE_PER_M = int(os.getenv("DEFAULT_FINE_TUNE_PRICE_PER_M"))
_DEFAULT_NUM_FINE_TUNE_EPOCHS = int(os.getenv("DEFAULT_NUM_FINE_TUNE_EPOCHS"))
_FORMATTED_TRAIN_EXAMPLES_URI = os.getenv("FORMATTED_TRAIN_EXAMPLES_URI")
_FORMATTED_VALIDATION_EXAMPLES_URI = os.getenv("FORMATTED_VALIDATION_EXAMPLES_URI")
_COMBINED_TRAIN_EXAMPLES_URI = os.getenv("COMBINED_TRAIN_EXAMPLES_URI")
_COMBINED_VALIDATION_EXAMPLES_URI = os.getenv("COMBINED_VALIDATION_EXAMPLES_URI")
_STOP_BUDGET_TASK_ID = "over_budget_stop"
_CONTINUE_TASK_ID = "in_budget_continue"
_STOP_NO_TRAIN_EXAMPLES_TASK_ID = "no_train_examples_stop"
_STOP_NO_VALIDATION_EXAMPLES_TASK_ID = "no_validation_examples_stop"
_GET_TRAIN_EXAMPLES_FILE_PATHS_TASK_ID = "get_train_examples_file_paths"
_GET_VALIDATION_EXAMPLES_FILE_PATHS_TASK_ID = "get_validation_examples_file_paths"


@dag(
    dag_display_name="🔍 Evaluate formatting and cost",
    start_date=datetime(2024, 4, 1),
    schedule=(
        Dataset(_FORMATTED_TRAIN_EXAMPLES_URI)
        | Dataset(_FORMATTED_VALIDATION_EXAMPLES_URI)
    ),
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["data_quality", "cost_control", "use-case"],
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=5),
        "owner": "MLE team",
    },
    params={
        "max_cost_allowed": Param(
            _DEFAULT_MAX_FINE_TUNING_COST,
            type="integer",
            description="The maximum cost allowed for fine-tuning.",
        ),
        "token_encoding": Param(
            _DEFAULT_TOKEN_ENCODING,
            type="string",
        ),
        "fine_tune_per_m_price": Param(
            _DEFAULT_FINE_TUNE_PRICE_PER_M,
            type="integer",
        ),
        "num_epochs": Param(
            _DEFAULT_NUM_FINE_TUNE_EPOCHS,
            type="integer",
        ),
    },
    doc_md=__doc__,
    description="Evaluate fine-tuning examples format and cost.",
)
def eval_and_cost_estimate():

    @task.branch
    def check_if_examples_exist(
        train_examples_folder_uri: str,
        validation_examples_folder_uri: str,
        stop_no_train_examples_task_id: str,
        stop_no_validation_examples_task_id: str,
        get_examples_task_ids: list[str],
    ) -> str:
        """
        Check if the training and validation examples exist.
        Args:
            train_examples_folder_uri (str): URI of the training example files folder.
            validation_examples_folder_uri (str): URI of the validation example files folder.
            stop_no_train_examples_task_id (str): Task ID to execute if no training examples exist.
            stop_no_validation_examples_task_id (str): Task ID to execute if no validation examples exist.
            get_examples_task_ids (list[str]): Task IDs to execute if examples exist.
        Returns:
            str/list: Task ID(s) of the next task(s) to execute.
        """
        train_examples_folder_path = train_examples_folder_uri.split("://")[1]
        validation_examples_folder_path = validation_examples_folder_uri.split("://")[1]

        if os.path.exists(train_examples_folder_path):
            if os.path.exists(validation_examples_folder_path):
                return get_examples_task_ids
            else:
                return stop_no_validation_examples_task_id
        elif os.path.exists(validation_examples_folder_path):
            return stop_no_train_examples_task_id
        else:
            return [stop_no_train_examples_task_id, stop_no_validation_examples_task_id]

    # TODO: Add a callback to send an alert to slack
    stop_no_train_examples = EmptyOperator(
        task_id=_STOP_NO_TRAIN_EXAMPLES_TASK_ID,
        on_success_callback=lambda x: x.log.info("Task stopped."),
    )
    stop_no_validation_examples = EmptyOperator(
        task_id=_STOP_NO_VALIDATION_EXAMPLES_TASK_ID,
        on_success_callback=lambda x: x.log.info("Task stopped."),
    )

    @task
    def get_file_paths_from_uri(examples_folder_uri: str) -> list[str]:
        """
        Get a list of file paths for the training examples.

        Args:
            train_examples_uri (str): URI of the training example files folder.
        Returns:
            list[str]: List of file paths in the directory.
        """

        examples_folder_file_path = examples_folder_uri.split("://")[1]

        file_paths = [
            os.path.join(examples_folder_file_path, f)
            for f in os.listdir(examples_folder_file_path)
            if os.path.isfile(os.path.join(examples_folder_file_path, f))
        ]
        return file_paths

    @task(
        map_index_template="{{ custom_map_index }}",
        max_active_tis_per_dag=1,  # protect against rate limiting of the tiktoken API
    )
    def check_examples_valid_formatting(example_file_path: str) -> list[str]:
        """
        Check the formatting of the examples in the file match fine-tuning requirements for GPT.
        Args:
            example_file_path (str): Path to the example file.
        Returns:
            list[str]: List of errors found in the example file if any.
        """

        with open(example_file_path, "r", encoding="utf-8") as f:
            dataset = [json.loads(line) for line in f]

        num_examples = len(dataset)
        format_errors = validate_example_format(dataset)

        t_log.info("First example, content:")
        for message in dataset[0]["messages"]:
            t_log.info(message)

        # set custom map index
        context = get_current_context()
        context["custom_map_index"] = (
            f"Parsing {num_examples} examples from: {example_file_path}"
        )

        return format_errors

    @task(
        map_index_template="{{ custom_map_index }}",
    )
    def count_tokens(example_file_path: str) -> int:
        """
        Count the number of billing tokens in a train examples file.
        Args:
            example_file_path (str): Path to the example file.
        Returns:
            int: Number of billing tokens in the dataset.
        """
        context = get_current_context()

        with open(example_file_path, "r", encoding="utf-8") as f:
            dataset = [json.loads(line) for line in f]

        encoding = context["params"]["token_encoding"]
        n_billing_tokens_in_dataset = dataset_token_analysis(dataset, encoding)

        # set custom map index
        context["custom_map_index"] = (
            f"{example_file_path} has {n_billing_tokens_in_dataset} billing tokens."
        )

        return n_billing_tokens_in_dataset

    @task
    def calculate_expected_cost(
        list_of_num_billing_tokens: list[int], **context
    ) -> float:
        """
        Calculate the expected cost of fine-tuning based on the number of tokens
        in the training examples, cost per million and epochs.

        Args:
            list_of_num_billing_tokens (list[int]): List of number of billing tokens in each dataset.
        Returns:
            float: Expected cost of fine-tuning.
        """
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
        total_cost: float, stop_task_id: str, continue_task_id: str, **context
    ) -> str:
        """
        Check if the total cost is within the budget.
        Args:
            total_cost (float): Total cost of fine-tuning.
            stop_task_id (str): Task ID to execute if the cost exceeds the budget.
            continue_task_id (str): Task ID to execute if the cost is within the budget.
        Returns:
            str: Task ID of the next task to execute.
        """
        max_cost_allowed = context["params"]["max_cost_allowed"]
        if total_cost <= max_cost_allowed:
            return continue_task_id
        else:
            return stop_task_id

    continue_task = EmptyOperator(task_id=_CONTINUE_TASK_ID)
    stop_budget_task = EmptyOperator(
        task_id=_STOP_BUDGET_TASK_ID,
        on_success_callback=lambda x: x.log.info("Task stopped."),
    )  # TODO: Add a callback to send an alert to slack

    @task
    def create_fine_tuning_example_file(
        example_file_paths: list[str], **context
    ) -> str:
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
    # Call tasks and define dependencies #
    # ---------------------------------- #

    check_if_examples_exist_obj = check_if_examples_exist(
        train_examples_folder_uri=_FORMATTED_TRAIN_EXAMPLES_URI,
        validation_examples_folder_uri=_FORMATTED_VALIDATION_EXAMPLES_URI,
        stop_no_train_examples_task_id=_STOP_NO_TRAIN_EXAMPLES_TASK_ID,
        stop_no_validation_examples_task_id=_STOP_NO_VALIDATION_EXAMPLES_TASK_ID,
        get_examples_task_ids=[
            _GET_TRAIN_EXAMPLES_FILE_PATHS_TASK_ID,
            _GET_VALIDATION_EXAMPLES_FILE_PATHS_TASK_ID,
        ],
    )

    get_train_examples_file_paths_obj = get_file_paths_from_uri.override(
        task_id=_GET_TRAIN_EXAMPLES_FILE_PATHS_TASK_ID
    )(examples_folder_uri=_FORMATTED_TRAIN_EXAMPLES_URI)
    get_validation_examples_file_paths_obj = get_file_paths_from_uri.override(
        task_id=_GET_VALIDATION_EXAMPLES_FILE_PATHS_TASK_ID
    )(examples_folder_uri=_FORMATTED_VALIDATION_EXAMPLES_URI)

    check_train_examples_valid_formatting_obj = (
        check_examples_valid_formatting.override(
            task_display_name="check_train_examples_valid_formatting",
        ).expand(example_file_path=get_train_examples_file_paths_obj)
    )

    check_validation_examples_valid_formatting_obj = (
        check_examples_valid_formatting.override(
            task_id="check_validation_examples_valid_formatting",
        ).expand(example_file_path=get_validation_examples_file_paths_obj)
    )

    count_tokens_obj = count_tokens.expand(
        example_file_path=get_train_examples_file_paths_obj
    )

    calculate_expected_cost_obj = calculate_expected_cost(count_tokens_obj)

    decide_if_cost_within_budget_obj = decide_if_cost_within_budget(
        total_cost=calculate_expected_cost_obj,
        stop_task_id=_STOP_BUDGET_TASK_ID,
        continue_task_id=_CONTINUE_TASK_ID,
    )

    create_fine_tuning_train_example_file_obj = (
        create_fine_tuning_example_file.override(
            task_id="create_fine_tuning_train_example_file",
            outlets=[Dataset(_COMBINED_TRAIN_EXAMPLES_URI)],
        )(example_file_paths=get_train_examples_file_paths_obj)
    )
    create_fine_tuning_validation_example_file_obj = (
        create_fine_tuning_example_file.override(
            task_id="create_fine_tuning_validation_example_file",
            outlets=[Dataset(_COMBINED_VALIDATION_EXAMPLES_URI)],
        )(example_file_paths=get_validation_examples_file_paths_obj)
    )

    chain(
        check_if_examples_exist_obj,
        [
            get_train_examples_file_paths_obj,
            get_validation_examples_file_paths_obj,
            stop_no_train_examples,
            stop_no_validation_examples,
        ],
    )
    chain(check_train_examples_valid_formatting_obj, count_tokens_obj)
    chain(decide_if_cost_within_budget_obj, [stop_budget_task, continue_task])
    chain(continue_task, create_fine_tuning_train_example_file_obj)
    chain(
        check_validation_examples_valid_formatting_obj,
        create_fine_tuning_validation_example_file_obj,
    )


eval_and_cost_estimate()
