from airflow.sdk import dag, task, Param, Variable


@dag(
    tags=["traditional-context-engineering"],
    params={
        "partial_grocery_list": Param(
            type="string",
            default="tofu, soy sauce, rice, mozzarella, tomatoes",
            description="Items you already plan to buy",
        ),
    },
)
def inference_dag():

    @task
    def load_preferences() -> str:
        return Variable.get("user_favorite_food", default="{}")

    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="""
        You are a helpful meal planning assistant. The user already has a
        partial grocery list started. Your job is to look at what they have
        so far, consider their favorite food, and suggest what else they
        should add to complete a full week of meals.

        Rules:
        - Don't repeat items already on the list.
        - Group your suggestions by category (produce, pantry, dairy, etc.).
        - Briefly explain why you're suggesting each addition
          (e.g. "goes well with the tofu for a stir fry").
        - At the end include one surprising adventurous pick that the user might not have thought of. 
        And label it as "Surprising adventurous pick".
        """,
    )
    def complete_grocery_list(context: str) -> str:
        return context

    @task
    def build_prompt(user_favorite_food: str, **context) -> str:
        partial_list = context["params"]["partial_grocery_list"]
        return (
            f"Here is the user's favorite food:\n{user_favorite_food}\n\n"
            f"Here is the user's partial grocery list:\n{partial_list}\n\n"
            f"What else should be added to the grocery list?"
        )

    @task
    def print_results(grocery_suggestions: str):
        print(grocery_suggestions)

    preferences = load_preferences()
    prompt = build_prompt(preferences)
    result = complete_grocery_list(prompt)
    print_results(result)


inference_dag()
