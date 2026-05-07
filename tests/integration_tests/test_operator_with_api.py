import pytest
from include.custom_operators.food_calculator import SelectIngredientsForMacrosOperator
from include.fridge_api.fridge_api import fridge_api


class TestOperatorWithRealAPI:

    @pytest.fixture
    def mock_context(self):
        return {}

    def test_operator_with_real_api_basic_flow(self, mock_context):
        fridge_contents = [
            {"type": "chicken breast", "quantity": 2},
            {"type": "rice", "quantity": 1.5},
            {"type": "broccoli", "quantity": 1},
            {"type": "eggs", "quantity": 3},
        ]

        desired_macros = {"protein": 60, "carbs": 50, "fat": 20, "calories": 600}

        operator = SelectIngredientsForMacrosOperator(
            task_id="integration_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert "selected_ingredients" in result
        assert "achieved_macros" in result
        assert "feasibility" in result
        assert len(result["selected_ingredients"]) > 0

    def test_operator_feasibility_check_accurate(self, mock_context):
        fridge_contents = [
            {"type": "chicken breast", "quantity": 1},
        ]

        desired_macros = {"protein": 100, "carbs": 50, "fat": 20, "calories": 800}

        operator = SelectIngredientsForMacrosOperator(
            task_id="feasibility_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert result["feasibility"]["protein"]["feasible"] is False
        assert result["available_macros"]["protein"] < desired_macros["protein"]

    def test_operator_selects_high_protein_items(self, mock_context):
        fridge_contents = [
            {"type": "chicken breast", "quantity": 2},
            {"type": "salmon", "quantity": 1},
            {"type": "tuna", "quantity": 1},
            {"type": "apple", "quantity": 3},
            {"type": "banana", "quantity": 2},
            {"type": "chocolate", "quantity": 1},
        ]

        desired_macros = {"protein": 80, "carbs": 20, "fat": 10, "calories": 500}

        operator = SelectIngredientsForMacrosOperator(
            task_id="high_protein_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        selected_types = [item["type"] for item in result["selected_ingredients"]]

        protein_rich_items = ["chicken breast", "salmon", "tuna"]
        selected_protein_items = [
            item for item in selected_types if item in protein_rich_items
        ]

        assert len(selected_protein_items) > 0

    def test_operator_with_vegetarian_options(self, mock_context):
        fridge_contents = [
            {"type": "tofu", "quantity": 2},
            {"type": "quinoa", "quantity": 1.5},
            {"type": "lentils", "quantity": 1},
            {"type": "chickpeas", "quantity": 1},
            {"type": "spinach", "quantity": 2},
            {"type": "avocado", "quantity": 1},
        ]

        desired_macros = {"protein": 40, "carbs": 60, "fat": 25, "calories": 600}

        operator = SelectIngredientsForMacrosOperator(
            task_id="vegetarian_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) >= 2
        assert result["achieved_macros"]["protein"] > 0
        assert result["achieved_macros"]["carbs"] > 0

    def test_operator_with_large_fridge(self, mock_context):
        fridge_contents = fridge_api.fetch_fridge_contents(num_items=20)

        desired_macros = {"protein": 50, "carbs": 70, "fat": 30, "calories": 700}

        operator = SelectIngredientsForMacrosOperator(
            task_id="large_fridge_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) >= 2
        expected_selection = max(2, int(len(fridge_contents) * 0.75))
        assert len(result["selected_ingredients"]) <= expected_selection

    def test_operator_achieved_macros_match_calculation(self, mock_context):
        fridge_contents = [
            {"type": "eggs", "quantity": 2},
            {"type": "bread", "quantity": 2},
            {"type": "butter", "quantity": 0.1},
        ]

        desired_macros = {"protein": 20, "carbs": 30, "fat": 15, "calories": 300}

        operator = SelectIngredientsForMacrosOperator(
            task_id="accuracy_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        manual_calculation = fridge_api.get_available_macros(
            result["selected_ingredients"]
        )

        assert result["achieved_macros"]["protein"] == manual_calculation["protein"]
        assert result["achieved_macros"]["carbs"] == manual_calculation["carbs"]
        assert result["achieved_macros"]["fat"] == manual_calculation["fat"]
        assert result["achieved_macros"]["calories"] == manual_calculation["calories"]

    def test_operator_with_random_fridge_contents(self, mock_context):
        fridge_contents = fridge_api.fetch_fridge_contents(num_items=10)

        desired_macros = {"protein": 40, "carbs": 60, "fat": 25, "calories": 550}

        operator = SelectIngredientsForMacrosOperator(
            task_id="random_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) > 0
        assert isinstance(result["achieved_macros"], dict)
        assert all(
            macro in result["achieved_macros"]
            for macro in ["protein", "carbs", "fat", "calories"]
        )

    def test_operator_balanced_meal(self, mock_context):
        fridge_contents = [
            {"type": "chicken breast", "quantity": 1.5},
            {"type": "rice", "quantity": 1},
            {"type": "broccoli", "quantity": 1.5},
            {"type": "olive oil", "quantity": 0.1},
        ]

        desired_macros = {"protein": 45, "carbs": 50, "fat": 20, "calories": 550}

        operator = SelectIngredientsForMacrosOperator(
            task_id="balanced_meal_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) >= 2

        total_protein = result["achieved_macros"]["protein"]
        total_carbs = result["achieved_macros"]["carbs"]
        total_fat = result["achieved_macros"]["fat"]
        total_macros = total_protein + total_carbs + total_fat

        if total_macros > 0:
            protein_ratio = total_protein / total_macros
            carbs_ratio = total_carbs / total_macros

            assert protein_ratio > 0
            assert carbs_ratio > 0

    def test_operator_high_carb_request(self, mock_context):
        fridge_contents = [
            {"type": "rice", "quantity": 2},
            {"type": "pasta", "quantity": 1.5},
            {"type": "oats", "quantity": 1},
            {"type": "banana", "quantity": 3},
            {"type": "chicken breast", "quantity": 1},
        ]

        desired_macros = {"protein": 30, "carbs": 100, "fat": 15, "calories": 650}

        operator = SelectIngredientsForMacrosOperator(
            task_id="high_carb_test",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        carb_rich_items = ["rice", "pasta", "oats", "banana"]
        selected_types = [item["type"] for item in result["selected_ingredients"]]
        carb_items_selected = [item for item in selected_types if item in carb_rich_items]

        assert len(carb_items_selected) > 0

    def test_operator_empty_fridge_returns_empty(self, mock_context):
        operator = SelectIngredientsForMacrosOperator(
            task_id="empty_test",
            fridge_contents=[],
            desired_macros={"protein": 50, "carbs": 60, "fat": 25, "calories": 600},
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) == 0
        assert result["achieved_macros"]["protein"] == 0.0
        assert result["achieved_macros"]["carbs"] == 0.0

