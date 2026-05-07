# Mocked Fridge API

from typing import Dict, List
import random
from include.fridge_api.nutrition_data import NUTRITION_DATABASE


class MockFridgeAPI:

    def __init__(self, base_url: str = "https://api.smartfridge.io"):
        self.base_url = base_url
        self.nutrition_db = NUTRITION_DATABASE

    def get_available_macros(self, fridge_contents: List[Dict]) -> Dict:
        total_macros = {
            "protein": 0.0,
            "carbs": 0.0,
            "fat": 0.0,
            "calories": 0.0,
        }

        for item in fridge_contents:
            item_type = item.get("type")
            quantity = item.get("quantity")
            nutrition = self.nutrition_db[item_type]

            total_macros["protein"] += nutrition["protein"] * quantity
            total_macros["carbs"] += nutrition["carbs"] * quantity
            total_macros["fat"] += nutrition["fat"] * quantity
            total_macros["calories"] += nutrition["calories"] * quantity

        for key in total_macros:
            total_macros[key] = round(total_macros[key], 2)

        return total_macros

    def get_ingredient_macros(
        self, ingredient_type: str, quantity: float = 1.0
    ) -> Dict:

        if ingredient_type not in self.nutrition_db:
            raise ValueError(f"Ingredient {ingredient_type} not found")

        nutrition = self.nutrition_db[ingredient_type]

        return {
            "ingredient": ingredient_type,
            "quantity": quantity,
            "unit": nutrition["unit"],
            "protein": round(nutrition["protein"] * quantity, 2),
            "carbs": round(nutrition["carbs"] * quantity, 2),
            "fat": round(nutrition["fat"] * quantity, 2),
            "calories": round(nutrition["calories"] * quantity, 2),
        }

    def fetch_fridge_contents(
        self, fridge_id: str = "default", num_items: int = 12
    ) -> List[Dict]:
        all_foods = list(self.nutrition_db.keys())

        selected_foods = random.sample(all_foods, min(num_items, len(all_foods)))

        fridge_contents = []
        for food in selected_foods:
            food_info = self.nutrition_db[food]
            unit = food_info["unit"]

            if unit == "piece":
                quantity = random.randint(1, 5)
            elif unit == "slice":
                quantity = random.randint(1, 8)
            elif "100g" in unit or "100ml" in unit:
                quantity = round(random.uniform(0.5, 5.0), 1)
            else:
                quantity = round(random.uniform(0.5, 3.0), 1)

            fridge_contents.append({"type": food, "units": unit, "quantity": quantity})

        return fridge_contents


fridge_api = MockFridgeAPI()
