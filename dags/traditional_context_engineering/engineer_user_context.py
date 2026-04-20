from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import dag, task, Param, chain, Variable


@dag(
    tags=["traditional-context-engineering"],
)
def engineer_user_context():

    _extract_user_favorite_food = HITLOperator(
        task_id="extract_user_favorite_food",
        subject="What is your favorite food?",
        body="",
        options=["Submit"],
        multiple=True,
        params={
            "Favorite food": Param(type="string", default="e.g. fondue, raclette, rösti, etc."),
        },
    )

    @task
    def load_user_favorite_food(hitl_output: dict):
        params = hitl_output["params_input"]
        preferences = {
            "favorite_food": params["Favorite food"],
        }
        import json
        Variable.set("user_favorite_food", json.dumps(preferences))

    chain(
        _extract_user_favorite_food,
        load_user_favorite_food(_extract_user_favorite_food.output),
    )


engineer_user_context()
