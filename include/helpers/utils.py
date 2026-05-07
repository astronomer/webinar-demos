from airflow.sdk import Param

DEFAULT_PARAMS = {
    "mood_input": Param(
        type="string",
        default="Something sweet and something savory",
    ),
    "food_type": Param(
        type="string",
        default="Breakfast",
        enum=["Breakfast", "Lunch", "Dinner", "Snack"],
        description="The type of food you want to eat",
    ),
    "desired_protein": Param(
        type="number",
        default=30,
        description="Target protein in grams",
    ),
    "desired_carbs": Param(
        type="number",
        default=50,
        description="Target carbohydrates in grams",
    ),
    "desired_fat": Param(
        type="number",
        default=20,
        description="Target fat in grams",
    ),
    "desired_calories": Param(
        type="number",
        default=500,
        description="Target calories",
    ),
}
