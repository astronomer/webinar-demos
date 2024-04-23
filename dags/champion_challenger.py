"""
## Retrieve accuracy of the champion and challenger models and switch if necessary.

This DAG compares the champion and challenger models' accuracy and switches the 
champion model if the challenger model has a higher accuracy. 
If no champion model exists, the challenger model becomes the champion model. 
If no challenger model exists, the champion model remains the champion model.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain, chain_linear
from pendulum import datetime, duration
import json
import os

# Variables used in the DAG
_CHALLENGER_MODEL_INFO_URI = os.getenv("CHALLENGER_MODEL_INFO_URI")
_CHAMPION_MODEL_INFO_URI = os.getenv("CHAMPION_MODEL_INFO_URI")
_CHAMPION_EXISTS_TASK_ID = "champion_exists"
_NO_CHAMPION_TASK_ID = "no_champion"
_CHALLENGER_EXISTS_TASK_ID = "challenger_exists"
_NO_CHALLENGER_TASK_ID = "no_challenger"
_KEEP_CHAMPION_TASK_ID = "keep_champion"
_SWITCH_CHAMPION_TASK_ID = "switch_champion"


@dag(
    dag_display_name="✨ Champion vs Challenger",
    start_date=datetime(2024, 4, 1),
    schedule=[Dataset(_CHALLENGER_MODEL_INFO_URI)],
    tags=["pick_model", "use-case"],
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=5),
        "owner": "MLE team",
    },
    doc_md=__doc__,
    description="Decide which model to use based on accuracy.",
)
def champion_challenger():

    # ------------------------------------------------- #
    # Check existence of champion and challenger models #
    # ------------------------------------------------- #

    @task.branch
    def is_champion(
        champion_model_info_uri: str,
        champion_exists_task_id: str,
        no_champion_task_id: str,
    ) -> str:
        """
        Check if a champion model exists. Determine DAG path based on the result.
        Args:
            champion_model_info_uri: The URI of the champion model info file.
            champion_exists_task_id: Task ID to execute if the champion model exists.
            no_champion_task_id: Task ID to execute if the champion model does not exist.
        Returns:
            str: Task ID of the next task to execute.
        """
        champion_model_info_file_path = champion_model_info_uri.split("://")[1]

        if os.path.exists(champion_model_info_file_path):
            return champion_exists_task_id

        return no_champion_task_id

    champion_exists = EmptyOperator(task_id=_CHAMPION_EXISTS_TASK_ID)
    no_champion = EmptyOperator(task_id=_NO_CHAMPION_TASK_ID)

    @task.branch
    def is_challenger(
        challenger_model_info_uri: str,
        challenger_exists_task_id: str,
        no_challenger_task_id: str,
    ) -> str:
        """
        Check if a challenger model exists. Determine DAG path based on the result.
        Args:
            challenger_model_info_uri: The URI of the challenger model info file.
            challenger_exists_task_id: Task ID to execute if the challenger model exists.
            no_challenger_task_id: Task ID to execute if the challenger model does not exist.
        Returns:
            str: Task ID of the next task to execute.
        """

        challenger_model_info_file_path = challenger_model_info_uri.split("://")[1]

        if os.path.exists(challenger_model_info_file_path):
            return challenger_exists_task_id

        return no_challenger_task_id

    challenger_exists = EmptyOperator(task_id=_CHALLENGER_EXISTS_TASK_ID)
    no_challenger = EmptyOperator(task_id=_NO_CHALLENGER_TASK_ID)

    start_the_battle = EmptyOperator(task_id="start_the_battle")

    # --------------------------------------- #
    # Get accuracy of champion and challenger #
    # --------------------------------------- #

    @task
    def get_challenger_accuracy(challenger_model_info_uri: str) -> float:
        """
        Get the accuracy of the challenger model.
        """

        challenger_model_info_file_path = challenger_model_info_uri.split("://")[1]

        with open(challenger_model_info_file_path, "r") as f:
            accuracy = json.load(f)["accuracy"]
        return accuracy

    @task
    def get_champion_accuracy(champion_model_info_uri: str) -> float:
        """
        Get the accuracy of the champion model.
        """
        champion_model_info_file_path = champion_model_info_uri.split("://")[1]

        with open(champion_model_info_file_path, "r") as f:
            accuracy = json.load(f)["accuracy"]
        return accuracy

    # -------------------------------------------- #
    # Compare and switch if challenger is superior #
    # -------------------------------------------- #

    @task.branch
    def compare_accuracies(
        champion_accuracy: float,
        challenger_accuracy: float,
        keep_champion_task_id: str,
        switch_champion_task_id: str,
    ) -> str:
        """
        Compare the accuracy of the champion and challenger models.
        """
        if champion_accuracy > challenger_accuracy:
            return keep_champion_task_id
        else:
            return switch_champion_task_id

    @task(task_id=_SWITCH_CHAMPION_TASK_ID, trigger_rule="one_success")
    def switch_champion(
        challenger_model_info_uri: str, champion_model_info_uri: str
    ) -> None:
        """
        Switch the champion model to the challenger model.
        """
        import shutil
        import os

        challenger_model_info_file_path = challenger_model_info_uri.split("://")[1]
        champion_model_info_file_path = champion_model_info_uri.split("://")[1]
        champion_model_info_dir = os.path.dirname(champion_model_info_file_path)

        if not os.path.exists(champion_model_info_dir):
            os.makedirs(champion_model_info_dir)

        shutil.move(
            challenger_model_info_file_path,
            champion_model_info_file_path,
        )

    keep_champion = EmptyOperator(
        task_id=_KEEP_CHAMPION_TASK_ID, trigger_rule="one_success"
    )

    # ---------------------------------- #
    # Call tasks and define dependencies #
    # ---------------------------------- #

    chain(
        is_champion(
            champion_model_info_uri=_CHAMPION_MODEL_INFO_URI,
            champion_exists_task_id=_CHAMPION_EXISTS_TASK_ID,
            no_champion_task_id=_NO_CHAMPION_TASK_ID,
        ),
        [champion_exists, no_champion],
    )

    chain(
        is_challenger(
            challenger_model_info_uri=_CHALLENGER_MODEL_INFO_URI,
            challenger_exists_task_id=_CHALLENGER_EXISTS_TASK_ID,
            no_challenger_task_id=_NO_CHALLENGER_TASK_ID,
        ),
        [challenger_exists, no_challenger],
    )

    get_champion_accuracy_obj = get_champion_accuracy(
        champion_model_info_uri=_CHAMPION_MODEL_INFO_URI
    )
    get_challenger_accuracy_obj = get_challenger_accuracy(
        challenger_model_info_uri=_CHALLENGER_MODEL_INFO_URI
    )

    comparison = compare_accuracies(
        champion_accuracy=get_champion_accuracy_obj,
        challenger_accuracy=get_challenger_accuracy_obj,
        keep_champion_task_id=_KEEP_CHAMPION_TASK_ID,
        switch_champion_task_id=_SWITCH_CHAMPION_TASK_ID,
    )
    switch_champion_obj = switch_champion(
        challenger_model_info_uri=_CHALLENGER_MODEL_INFO_URI,
        champion_model_info_uri=_CHAMPION_MODEL_INFO_URI,
    )

    chain(challenger_exists, get_challenger_accuracy_obj)
    chain_linear(
        [champion_exists, challenger_exists],
        start_the_battle,
        [get_champion_accuracy_obj, get_challenger_accuracy_obj],
    )
    chain(comparison, [keep_champion, switch_champion_obj])
    chain(no_champion, switch_champion_obj)
    chain(no_challenger, keep_champion)


champion_challenger()
