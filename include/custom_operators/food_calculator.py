from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.hook import BaseHook
from include.fridge_api.fridge_api import fridge_api
from typing import Dict, List


class SelectIngredientsForMacrosOperator(BaseOperator):

    template_fields = ("fridge_contents", "desired_macros")

    def __init__(self, fridge_contents: list, desired_macros: dict, **kwargs):
        super().__init__(**kwargs)
        self.fridge_contents = fridge_contents
        self.desired_macros = desired_macros

    def execute(self, context):
        self.log.info(f"Desired macros: {self.desired_macros}")
        self.log.info(f"Available items in fridge: {len(self.fridge_contents)}")

        available_macros = fridge_api.get_available_macros(self.fridge_contents)
        self.log.info(f"Total available macros of fridge: {available_macros}")

        feasibility_check = self._check_feasibility(available_macros)

        selected_ingredients = self._select_ingredients()

        achieved_macros = fridge_api.get_available_macros(selected_ingredients)

        result = {
            "selected_ingredients": selected_ingredients,
            "desired_macros": self.desired_macros,
            "achieved_macros": achieved_macros,
            "available_macros": available_macros,
            "feasibility": feasibility_check,
        }

        self.log.info(f"Selected {len(selected_ingredients)} ingredient options:")
        for ingredient in selected_ingredients:
            ingredient_macros = fridge_api.get_ingredient_macros(
                ingredient.get("type"), ingredient.get("quantity")
            )
            self.log.info(
                f"  - {ingredient.get('type')} (qty: {ingredient.get('quantity')}): "
                f"protein={ingredient_macros.get('protein')}g, "
                f"carbs={ingredient_macros.get('carbs')}g, "
                f"fat={ingredient_macros.get('fat')}g, "
                f"calories={ingredient_macros.get('calories')}"
            )
        self.log.info(f"Total of selected ingredient options: {achieved_macros}")

        return result

    def _check_feasibility(self, available_macros: Dict) -> Dict:
        feasibility = {}

        for macro in ["protein", "carbs", "fat", "calories"]:
            desired = self.desired_macros.get(macro, 0)
            available = available_macros.get(macro, 0)

            if desired > available:
                feasibility[macro] = {
                    "feasible": False,
                    "shortfall": round(desired - available, 2),
                }
            else:
                feasibility[macro] = {
                    "feasible": True,
                    "surplus": round(available - desired, 2),
                }

        return feasibility

    def _select_ingredients(self) -> List[Dict]:
        total_desired = sum(
            [
                self.desired_macros.get("protein", 0),
                self.desired_macros.get("carbs", 0),
                self.desired_macros.get("fat", 0),
            ]
        )

        if total_desired == 0:
            return []

        desired_ratios = {
            "protein": self.desired_macros.get("protein", 0) / total_desired,
            "carbs": self.desired_macros.get("carbs", 0) / total_desired,
            "fat": self.desired_macros.get("fat", 0) / total_desired,
        }

        scored_items = []
        for item in self.fridge_contents:
            item_macros = fridge_api.get_ingredient_macros(
                item.get("type"), item.get("quantity")
            )

            if "error" not in item_macros:
                score = self._score_ingredient(item_macros, desired_ratios)
                scored_items.append((item, score))

        scored_items.sort(key=lambda x: x[1], reverse=True)
        num_to_select = max(2, int(len(scored_items) * 0.75))

        selected = [item for item, score in scored_items[:num_to_select]]

        return selected

    def _score_ingredient(self, item_macros: Dict, desired_ratios: Dict) -> float:
        total_macros = (
            item_macros.get("protein", 0)
            + item_macros.get("carbs", 0)
            + item_macros.get("fat", 0)
        )

        if total_macros == 0:
            return 0

        item_ratios = {
            "protein": item_macros.get("protein", 0) / total_macros,
            "carbs": item_macros.get("carbs", 0) / total_macros,
            "fat": item_macros.get("fat", 0) / total_macros,
        }

        differences = sum(
            [
                (item_ratios[macro] - desired_ratios[macro]) ** 2
                for macro in ["protein", "carbs", "fat"]
            ]
        )

        score = 100 / (1 + differences * 10)

        return score
