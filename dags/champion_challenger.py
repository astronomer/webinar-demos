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
from airflow.models.baseoperator import chain
from pendulum import datetime
import os

_CHALLENGER_MODEL_INFO_URI = os.getenv("CHALLENGER_MODEL_INFO_URI")
_CHAMPION_MODEL_INFO_URI = os.getenv("CHAMPION_MODEL_INFO_URI")
_CHAMPION_EXISTS_TASK_ID = "champion_exists"
_NO_CHAMPION_TASK_ID = "no_champion"


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=[Dataset(_CHALLENGER_MODEL_INFO_URI)],
    tags=["ingest"],
)
def champion_challenger():

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
        Returns:
            str: Task ID of the next task to execute.
        """
        import os

        champion_model_info_file_path = champion_model_info_uri.split("://")[1]

        if os.path.exists(champion_model_info_file_path):
            return champion_exists_task_id

        return no_champion_task_id

    champion_exists = EmptyOperator(task_id=_CHAMPION_EXISTS_TASK_ID)
    no_champion = EmptyOperator(task_id=_NO_CHAMPION_TASK_ID)

    chain(
        is_champion(
            champion_model_info_uri=_CHAMPION_MODEL_INFO_URI,
            champion_exists_task_id=_CHAMPION_EXISTS_TASK_ID,
            no_champion_task_id=_NO_CHAMPION_TASK_ID,
        ),
        [champion_exists, no_champion],
    )

    @task.branch
    def is_challenger():
        """
        Check if a challenger model exists.
        """
        import os

        if os.path.exists("include/model_results/challenger/challenger_accuracy.json"):
            return "challenger_exists"

        return "no_challenger"

    challenger_exists = EmptyOperator(task_id="challenger_exists")
    no_challenger = EmptyOperator(task_id="no_challenger")

    chain(is_challenger(), [challenger_exists, no_challenger])

    @task
    def get_challenger_accuracy():
        """
        Get the accuracy of the challenger model.
        """
        import json

        with open(
            "include/model_results/challenger/challenger_accuracy.json", "r"
        ) as f:
            accuracy = json.load(f)["accuracy"]
        return accuracy

    @task
    def get_champion_accuracy():
        """
        Get the accuracy of the champion model.
        """
        import json

        with open("include/model_results/champion/champion_accuracy.json", "r") as f:
            accuracy = json.load(f)["accuracy"]
        return accuracy

    @task.branch
    def compare_accuracies(champion_accuracy, challenger_accuracy):
        """
        Compare the accuracy of the champion and challenger models.
        """
        if champion_accuracy > challenger_accuracy:
            return "keep_champion"
        else:
            return "switch_champion"

    @task(trigger_rule="one_success")
    def switch_champion():
        """
        Switch the champion model to the challenger model.
        """
        import shutil
        import os

        if not os.path.exists("include/model_results/champion"):
            os.makedirs("include/model_results/champion")

        shutil.move(
            "include/model_results/challenger/challenger_accuracy.json",
            "include/model_results/champion/champion_accuracy.json",
        )

    keep_champion = EmptyOperator(task_id="keep_champion", trigger_rule="one_success")

    get_champion_accuracy_obj = get_champion_accuracy()
    get_challenger_accuracy_obj = get_challenger_accuracy()

    chain(challenger_exists, get_challenger_accuracy_obj)
    chain(champion_exists, [get_champion_accuracy_obj, get_challenger_accuracy_obj])

    comparison = compare_accuracies(
        get_champion_accuracy_obj, get_challenger_accuracy_obj
    )
    switch_champion_obj = switch_champion()

    chain(comparison, [keep_champion, switch_champion_obj])
    chain(no_champion, switch_champion_obj)
    chain(no_challenger, keep_champion)


champion_challenger()
