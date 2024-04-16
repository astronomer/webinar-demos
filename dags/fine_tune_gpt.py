"""
## Fine-tune GPT-3

"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.param import Param
from airflow.sensors.base import PokeReturnValue
from airflow.models.baseoperator import chain
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=[Dataset("include/examples/fine_tune_examples/")],
    catchup=False,
    tags=["ml_team"],
    default_args={
        "retries": 0,
        "owner": "Astronomer",
    },
    params={
        "manual_choice_examples_file": Param(
            False,
            type="boolean",
            description="Switch to True if you want to manually choose examples for fine-tuning. Provide the path to the file in the 'manual_choice_examples_file_path' parameter.",
        ),
        "manual_choice_examples_file_path": Param(
            "include/examples/fine_tune_examples/20240416T113231_combined_examples.json",
            type=["null", "string"],
            description="Path to the file containing examples for fine-tuning.",
        ),
        "manual_model_fine_tuning_suffix": Param(
            None,
            type=["null", "string"],
            description="Optional. Manually set the suffix for the fine-tuning job. If not set, the suffix will be generated automatically based on the DAG run id. Max 18 characters.",
            maxLength=18,
        ),
    },
)
def fine_tune_gpt():

    @task
    def choose_examples_file(**context):

        if context["params"]["manual_choice_examples_file"]:
            examples_file_path = context["params"]["manual_choice_examples_file_path"]

        else:
            examples_file_path = context["ti"].xcom_pull(
                task_ids="create_fine_tuning_example_file",
                dag_id="evaluate_formatting_fine_tuning_examples",
                include_prior_dates=True,
            )

        return examples_file_path

    @task
    def upload_examples_file_to_openai(examples_file_path):
        pass

        from openai import OpenAI

        client = OpenAI()  # fetches the API key from the environment var OPENAI_API_KEY
        r = client.files.create(
            file=open(examples_file_path, "rb"), purpose="fine-tune"
        )

        print(r)

        t_log.info(f"Uploaded examples file to OpenAI. File ID: {r.id}")

        return r.id

    # TODO: Create a deferrable operator for this
    @task
    def fine_tune_gpt_model(fine_tuning_file_id, validation_file_id, **context):

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

        print(fine_tune_info)

        return fine_tune_info.id

    @task.sensor
    def wait_for_model_fine_tuning_to_complete(fine_tune_id, **context):
        from openai import OpenAI
        from pendulum import from_timestamp

        client = OpenAI()
        fine_tune_info = client.fine_tuning.jobs.retrieve(fine_tune_id)

        if fine_tune_info.status == "succeeded":

            fine_tuning_duration_seconds = (
                fine_tune_info.finished_at - fine_tune_info.created_at
            )
            fine_tuning_duration_hr = fine_tuning_duration_seconds // 3600
            fine_tuning_duration_min = (fine_tuning_duration_seconds % 3600) // 60
            fine_tuning_duration_s = (fine_tuning_duration_seconds % 3600) % 60

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

    @task
    def get_model_results(result_files, model_info, **context):
        from openai import OpenAI
        import pandas as pd
        import matplotlib.pyplot as plt
        import os

        client = OpenAI()

        os.makedirs("include/results/plots", exist_ok=True)
        ts = context["ts_nodash"]

        validation_mean_token_acc_list = []

        fine_tuned_model = context["ti"].xcom_pull(
            task_ids="wait_for_model_fine_tuning_to_complete", key="fine_tuned_model"
        )

        for file in result_files:
            result_file_info = client.files.retrieve_content(file)
            from io import StringIO

            df = pd.read_csv(StringIO(result_file_info))

            # TODO: prettify
            plt.figure(figsize=(12, 10))

            plt.suptitle(f"Fine-tuning Results: {fine_tuned_model}", fontsize=16)

            plt.subplot(2, 2, 1)
            plt.plot(df["step"], df["train_loss"], label="Training Loss", color="blue")
            plt.xlabel("Training Step")
            plt.ylabel("Loss")
            plt.title("Training Loss Over Time")
            plt.legend()

            plt.subplot(2, 2, 2)
            plt.plot(
                df["step"],
                df["train_accuracy"],
                label="Training Accuracy",
                color="green",
            )
            plt.xlabel("Training Step")
            plt.ylabel("Accuracy")
            plt.title("Training Accuracy Over Time")
            plt.legend()

            plt.subplot(2, 2, 3)
            plt.plot(df["step"], df["valid_loss"], label="Validation Loss", color="red")
            plt.xlabel("Training Step")
            plt.ylabel("Loss")
            plt.title("Validation Loss Over Time")
            plt.legend()

            plt.subplot(2, 2, 4)
            plt.plot(
                df["step"],
                df["valid_mean_token_accuracy"],
                label="Validation Mean Token Accuracy",
                color="purple",
            )
            plt.xlabel("Training Step")
            plt.ylabel("Accuracy")
            plt.title("Validation Mean Token Accuracy Over Time")
            plt.legend()

            plt.tight_layout()
            plt.savefig(f"include/results/plots/{ts}_fine_tuning_results.png")
            last_validation_mean_token_acc = df["valid_mean_token_accuracy"].iloc[-1]
            validation_mean_token_acc_list.append(last_validation_mean_token_acc)

        return validation_mean_token_acc_list

    @task
    def save_fine_tuned_model_id(**context):

        fine_tuned_model = context["ti"].xcom_pull(
            task_ids="wait_for_model_fine_tuning_to_complete", key="fine_tuned_model"
        )

        with open("include/results/production_model_id.txt", "w") as f:
            f.write(fine_tuned_model)

    fine_tune_gpt_model_obj = fine_tune_gpt_model(
        fine_tuning_file_id=upload_examples_file_to_openai(
            examples_file_path=choose_examples_file()
        ),
        validation_file_id="file-L6AANC8C08nIfrUlN9JyQl3Y",  # TODO: make this part of the DAG
    )

    get_model_results_obj = get_model_results(
        wait_for_model_fine_tuning_to_complete(fine_tune_gpt_model_obj),
        model_info=fine_tune_gpt_model_obj,
    )

    chain(get_model_results_obj, save_fine_tuned_model_id())


fine_tune_gpt()
