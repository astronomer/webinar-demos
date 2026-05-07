from airflow.sdk import dag, task, Param, Asset, chain
import json
import os
from airflow.providers.standard.operators.hitl import (
    HITLBranchOperator,
    HITLEntryOperator,
)
from include.custom_operators.food_calculator import (
    SelectIngredientsForMacrosOperator,
)
from datetime import timedelta
from include.helpers.utils import DEFAULT_PARAMS

CHEF_SYSTEM_PROMPT = """
    You are a legendary chef creating a recipe based on the user's mood,
    desired food type, and the available ingredients in the
    fridge that most closely matches the desired macros.

    You will be given the following information:
    - selected_ingredients_data: Contains selected ingredient alongisde their macros
    - desired_macros: The desired macros the user wants to achieve
    - mood_input: User's mood/cravings
    - food_type: Type of meal (Breakfast, Lunch, Dinner, Snack)

    You don't have to use all the ingredients provided, you can also use a subset.
    Also you are free to create several dishes in the same recipe as long
    as the combined macros of the dishes match the desired macros.

    You can assume a well stocked pantry of spices and helper ingredients.
    Make sure to markdown format the recipe nicely.
"""

EDITOR_SYSTEM_PROMPT = """You are a recipe editor making changes to a recipe based on the user's feedback.
Make sure to markdown format the recipe nicely even if no changes are needed to the contents."""

OUTPUT_LOCATION = os.getenv("OUTPUT_LOCATION", "include/recipes/recipe.txt")


@dag(
    schedule=[Asset("start_over_asset")],
    params=DEFAULT_PARAMS,
    max_consecutive_failed_dag_runs=3,
    tags=["recipe_generation"],
)
def generate_recipe():

    @task
    def get_food_params(**context):
        if str(context["dag_run"].run_type) == "DagRunType.ASSET_TRIGGERED":
            for asset in context["triggering_asset_events"][Asset("start_over_asset")]:
                food_params = asset.extra["params"]
                print(f"Food params from start over asset: {food_params}")
                food_params = json.loads(food_params)
                return food_params
        if str(context["dag_run"].run_type) == "DagRunType.MANUAL":
            food_params = context["params"]
            print(f"Food params from manual run params: {food_params}")
            return dict(food_params)

    _get_food_params = get_food_params()

    @task
    def get_fridge_contents():
        from include.fridge_api.fridge_api import fridge_api

        fridge_contents = fridge_api.fetch_fridge_contents()
        return fridge_contents

    _get_fridge_contents = get_fridge_contents()

    @task
    def get_desired_macros(food_params: dict) -> dict:
        return {
            "protein": food_params["desired_protein"],
            "carbs": food_params["desired_carbs"],
            "fat": food_params["desired_fat"],
            "calories": food_params["desired_calories"],
        }

    _get_desired_macros = get_desired_macros(
        food_params=_get_food_params,
    )

    _select_ingredients_for_macros = SelectIngredientsForMacrosOperator(
        task_id="select_ingredients_for_macros",
        fridge_contents=_get_fridge_contents,
        desired_macros=_get_desired_macros,
    )

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=CHEF_SYSTEM_PROMPT,
        execution_timeout=timedelta(minutes=15),
    )
    def generate_recipe_draft(
        selected_ingredients_data: dict,
        desired_macros: dict,
        **context,
    ):
        mood_input = context["params"]["mood_input"]
        food_type = context["params"]["food_type"]
        return str(
            json.dumps(
                {
                    "selected_ingredients_data": selected_ingredients_data,
                    "mood_input": mood_input,
                    "food_type": food_type,
                    "desired_macros": desired_macros,
                }
            )
        )

    _generate_recipe_draft = generate_recipe_draft(
        selected_ingredients_data=_select_ingredients_for_macros.output,
        desired_macros=_get_desired_macros,
    )

    _human_edit_recipe = HITLEntryOperator(
        task_id="human_edit_recipe",
        subject="Recipe Edit Required",
        body=f"{_generate_recipe_draft}",
        params={
            "Changes to make:": Param(
                type="string",
                default="None! The recipe is perfect.",
            )
        },
    )

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=EDITOR_SYSTEM_PROMPT,
        execution_timeout=timedelta(minutes=10),
    )
    def edit_recipe(recipe_draft, hitl_output) -> str:
        change_requests = hitl_output["params_input"]["Changes to make:"]
        return str(
            json.dumps(
                {
                    "recipe_draft": recipe_draft,
                    "change_requests": change_requests,
                }
            )
        )

    _edit_recipe = edit_recipe(
        _generate_recipe_draft,
        _human_edit_recipe.output,
    )

    _human_approve_recipe = HITLBranchOperator(
        task_id="human_approve_recipe",
        subject="Recipe Approval Required",
        body=f"{_edit_recipe}",
        options=["Approve Recipe", "Try Again!"],
        defaults=["Approve Recipe"],
        options_mapping={
            "Approve Recipe": "finalize_recipe_output",
            "Try Again!": "try_again",
        },
        trigger_rule="all_success",
    )

    @task(outlets=[Asset("start_over_asset")])
    def try_again(**context):
        from airflow.sdk import Metadata

        yield Metadata(
            Asset("start_over_asset"), {"params": json.dumps(context["params"])}
        )
        print("Restarted the Dag!")

    _try_again = try_again()

    @task
    def finalize_recipe_output(
        final_recipe: str,
    ) -> dict:
        result = {
            "recipe": final_recipe,
        }

        os.makedirs(os.path.dirname(OUTPUT_LOCATION), exist_ok=True)

        with open(OUTPUT_LOCATION, "w") as f:
            f.write(final_recipe)

        print(f"Recipe saved to: {OUTPUT_LOCATION}")

        return result

    _finalize_recipe_output = finalize_recipe_output(
        _edit_recipe,
    )

    chain(
        _generate_recipe_draft,
        _human_edit_recipe,
        _edit_recipe,
        _human_approve_recipe,
        [_try_again, _finalize_recipe_output],
    )


generate_recipe()
