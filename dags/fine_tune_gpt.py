"""
## Fine-tune GPT-3-turbo using custom examples

Fine-tunes GPT-3 turbo on the latest train and validation examples. 
Manual runs offer the option to choose the specific example files.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.param import Param
from airflow.sensors.base import PokeReturnValue
from pendulum import datetime, duration
import logging
import os

t_log = logging.getLogger("airflow.task")

_COMBINED_TRAIN_EXAMPLES_URI = os.getenv("COMBINED_TRAIN_EXAMPLES_URI")
_COMBINED_VALIDATION_EXAMPLES_URI = os.getenv("COMBINED_VALIDATION_EXAMPLES_URI")
_PLOTS_URI = os.getenv("PLOTS_URI")
_CHALLENGER_MODEL_INFO_URI = os.getenv("CHALLENGER_MODEL_INFO_URI")


@dag(
    dag_display_name="🤖 Fine-tune GPT-3.5-turbo",
    start_date=datetime(2024, 4, 1),
    schedule=(
        Dataset(_COMBINED_TRAIN_EXAMPLES_URI)
        & Dataset(_COMBINED_VALIDATION_EXAMPLES_URI)
    ),
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["GPT-3.5-turbo"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "owner": "MLE team",
    },
    params={
        "manual_choice_train_examples_file": Param(
            False,
            type="boolean",
            description="Switch to True if you want to manually choose examples for fine-tuning. Provide the path to the file in the 'manual_choice_train_examples_file_path' parameter.",
        ),
        "manual_choice_train_examples_file_path": Param(
            None,
            type=["null", "string"],
            description="Path to the file containing examples for fine-tuning. Example: `include/examples/train_examples/fine_tune_examples/20240417T125742_combined_examples.json`",
        ),
        "manual_choice_validation_examples_file": Param(
            False,
            type="boolean",
            description="Switch to True if you want to manually choose examples for fine-tuning. Provide the path to the file in the 'manual_choice_validation_examples_file_path' parameter.",
        ),
        "manual_choice_validation_examples_file_path": Param(
            None,
            type=["null", "string"],
            description="Path to the file containing examples for fine-tuning. Example: `include/examples/validation_examples/fine_tune_examples/20240417T125742_combined_examples.json`",
        ),
        "manual_model_fine_tuning_suffix": Param(
            None,
            type=["null", "string"],
            description="Optional. Manually set the suffix for the fine-tuning job. If not set, the suffix will be generated automatically based on the DAG run id. Max 18 characters.",
            maxLength=18,
        ),
    },
    doc_md=__doc__,
    description="Fine tune GPT-3.5-turbo.",
)
def fine_tune_gpt():

    @task
    def choose_examples_file(type: str, **context) -> str:
        """
        Determine which examples file to use.
        Returns:
            str: Path to the chosen examples file.
        """

        if context["params"][f"manual_choice_{type}_examples_file"]:
            examples_file_path = context["params"][
                f"manual_choice_{type}_examples_file_path"
            ]

        else:
            examples_file_path = context["ti"].xcom_pull(
                task_ids=f"create_fine_tuning_{type}_example_file",
                dag_id="eval_and_cost_estimate",
                include_prior_dates=True,
            )

        return examples_file_path

    @task
    def upload_examples_file_to_openai(examples_file_path: str) -> str:
        """
        Uploads an examples file to OpenAI.
        Args:
            examples_file_path (str): Path to the examples file.
        Returns:
            str: File ID of the uploaded examples file.
        """

        from openai import OpenAI

        client = OpenAI()  # fetches the API key from the environment var OPENAI_API_KEY
        r = client.files.create(
            file=open(examples_file_path, "rb"), purpose="fine-tune"
        )

        t_log.info(f"Uploaded examples file to OpenAI. File ID: {r.id}")
        t_log.info(f"File name: {r.filename}")
        t_log.info(f"File size: {round(r.bytes / 1000, 2)} KB")

        return r.id

    # TODO: Create a deferrable operator for this
    @task
    def fine_tune_gpt_model(
        fine_tuning_file_id: str, validation_file_id: str, **context
    ) -> str:
        """
        Fine-tunes GPT-3.5-turbo on the provided examples.
        Args:
            fine_tuning_file_id (str): File ID of the examples to fine-tune on.
            validation_file_id (str): File ID of the examples to validate on.
        Returns:
            str: ID of the fine-tuning job.
        """

        from openai import OpenAI

        client = OpenAI()  # fetches the API key from the environment var OPENAI_API_KEY

        if context["params"]["manual_model_fine_tuning_suffix"]:
            suffix = context["params"]["manual_model_fine_tuning_suffix"]
        else:
            suffix = context["ts_nodash"]

        fine_tune_info = client.fine_tuning.jobs.create(
            training_file=fine_tuning_file_id,
            validation_file=validation_file_id,
            model="gpt-3.5-turbo",
            suffix=suffix,
        )

        t_log.info(f"Fine-tuning job created. Job ID: {fine_tune_info.id}")

        return fine_tune_info.id

    @task.sensor
    def wait_for_model_fine_tuning_to_complete(
        fine_tune_id: str, **context
    ) -> PokeReturnValue:
        """
        Waits for the fine-tuning job to complete.
        Args:
            fine_tune_id (str): ID of the fine-tuning job.
        Returns:
            PokeReturnValue: Whether the fine-tuning job is done + XCom value.
        """
        from openai import OpenAI
        from pendulum import from_timestamp

        client = OpenAI()
        fine_tune_info = client.fine_tuning.jobs.retrieve(fine_tune_id)

        if fine_tune_info.status == "succeeded":

            # compute duration
            fine_tuning_duration_seconds = (
                fine_tune_info.finished_at - fine_tune_info.created_at
            )
            fine_tuning_duration_hr = fine_tuning_duration_seconds // 3600
            fine_tuning_duration_min = (fine_tuning_duration_seconds % 3600) // 60
            fine_tuning_duration_s = (fine_tuning_duration_seconds % 3600) % 60

            # log fine tuning info
            t_log.info("Fine-tuning job completed successfully.")
            t_log.info("--- Fine-tuning Job Info ---")
            t_log.info(f"Fine-tuned model: {fine_tune_info.fine_tuned_model}")
            t_log.info(f"Model: {fine_tune_info.model}")
            t_log.info(f"Model ID: {fine_tune_info.id}")
            t_log.info(f"Organization ID: {fine_tune_info.organization_id}")
            t_log.info(f"Trained tokens: {fine_tune_info.trained_tokens}")
            t_log.info(f"Training file: {fine_tune_info.training_file}")
            t_log.info(
                f"Hyperparameters: {fine_tune_info.hyperparameters.n_epochs} epochs, "
                + f"batch size: {fine_tune_info.hyperparameters.batch_size}, "
                + f"learning rate multiplier: {fine_tune_info.hyperparameters.learning_rate_multiplier}"
            )
            t_log.info(f"Result file ids: {fine_tune_info.result_files}")

            t_log.info("--- Fine tuning Duration ---")
            t_log.info(f"Created at: {from_timestamp(fine_tune_info.created_at)}")
            t_log.info(f"Finished at: {from_timestamp(fine_tune_info.finished_at)}")
            t_log.info(
                f"Fine-tuning Duration: {fine_tuning_duration_hr} hr {fine_tuning_duration_min} min {fine_tuning_duration_s} s"
            )

            context["ti"].xcom_push("fine_tuned_model", fine_tune_info.fine_tuned_model)

            is_done = True
        else:
            if fine_tune_info.status == "failed":
                raise Exception(
                    "Fine-tuning job failed: "
                    + f"Status: {fine_tune_info.error.code}"
                    + f"{fine_tune_info.error.message}"
                )
            elif fine_tune_info.status == "cancelled":
                t_log.info("Fine-tuning job cancelled.")
                is_done = True
            else:
                t_log.info(
                    f"Fine-tuning job still in progress. Status: {fine_tune_info.status}"
                )
                is_done = False

        return PokeReturnValue(is_done=is_done, xcom_value=fine_tune_info.result_files)

    @task(
        outlets=[
            Dataset(_PLOTS_URI),
            Dataset(_CHALLENGER_MODEL_INFO_URI),
        ]
    )
    def get_model_model_results(result_files: list[str], **context) -> list[float]:
        """
        Get the results of the fine-tuning job. Plot them and save them to a file.
        Args:
            result_files (List[str]): List of file IDs containing the fine-tuning results.
        Returns:
            List[float]: List of the last validation mean token accuracy for each result file.
        """
        from openai import OpenAI
        import pandas as pd
        import os
        import json

        from include.task_functions.plotting import plot_model_train_val_graph

        client = OpenAI()

        os.makedirs("include/model_results/plots", exist_ok=True)
        ts = context["ts_nodash"]

        validation_mean_token_acc_list = []

        fine_tuned_model = context["ti"].xcom_pull(
            task_ids="wait_for_model_fine_tuning_to_complete", key="fine_tuned_model"
        )

        for file in result_files:
            result_file_info = client.files.retrieve_content(file)
            from io import StringIO

            df = pd.read_csv(StringIO(result_file_info))

            plot_model_train_val_graph(fine_tuned_model, df, ts)

            last_validation_mean_token_acc = df["valid_mean_token_accuracy"].iloc[-1]
            validation_mean_token_acc_list.append(last_validation_mean_token_acc)

            os.makedirs("include/model_results/challenger", exist_ok=True)

            with open(
                "include/model_results/challenger/challenger_accuracy.json", "w"
            ) as f:
                f.write(
                    json.dumps(
                        {
                            "challenger_model_id": fine_tuned_model,
                            "accuracy": last_validation_mean_token_acc,
                        }
                    )
                )
        return validation_mean_token_acc_list

    # ---------------------------------- #
    # Call tasks and define dependencies #
    # ---------------------------------- #

    upload_train_examples_file_to_openai_obj = upload_examples_file_to_openai.override(
        task_id="upload_train_examples_file_to_openai"
    )(
        examples_file_path=choose_examples_file.override(
            task_id="choose_train_examples_file"
        )(type="train")
    )

    upload_validation_examples_file_to_openai_obj = (
        upload_examples_file_to_openai.override(
            task_id="upload_validation_examples_file_to_openai"
        )(
            examples_file_path=choose_examples_file.override(
                task_id="choose_validation_examples_file"
            )(type="validation")
        )
    )

    fine_tune_gpt_model_obj = fine_tune_gpt_model(
        fine_tuning_file_id=upload_train_examples_file_to_openai_obj,
        validation_file_id=upload_validation_examples_file_to_openai_obj,
    )

    get_model_model_results(
        wait_for_model_fine_tuning_to_complete(fine_tune_gpt_model_obj),
        model_info=fine_tune_gpt_model_obj,
    )


fine_tune_gpt()
