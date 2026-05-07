import pytest
from unittest.mock import Mock, patch, MagicMock
from include.custom_operators.food_calculator import SelectIngredientsForMacrosOperator


class TestSelectIngredientsForMacrosOperator:

    @pytest.fixture
    def mock_context(self):
        return {}

    @pytest.fixture
    def sample_fridge_contents(self):
        return [
            {"type": "chicken breast", "units": "100g", "quantity": 2},
            {"type": "rice", "units": "100g", "quantity": 1},
            {"type": "broccoli", "units": "100g", "quantity": 1.5},
            {"type": "eggs", "units": "piece", "quantity": 3},
        ]

    @pytest.fixture
    def sample_desired_macros(self):
        return {"protein": 50, "carbs": 40, "fat": 20, "calories": 500}

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_execute_returns_correct_structure(
        self, mock_fridge_api, mock_context, sample_fridge_contents, sample_desired_macros
    ):
        mock_fridge_api.get_available_macros.return_value = {
            "protein": 100.0,
            "carbs": 80.0,
            "fat": 40.0,
            "calories": 1000.0,
        }
        mock_fridge_api.get_ingredient_macros.return_value = {
            "protein": 31.0,
            "carbs": 0.0,
            "fat": 3.6,
            "calories": 165.0,
        }

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_select_ingredients",
            fridge_contents=sample_fridge_contents,
            desired_macros=sample_desired_macros,
        )

        result = operator.execute(mock_context)

        assert "selected_ingredients" in result
        assert "desired_macros" in result
        assert "achieved_macros" in result
        assert "available_macros" in result
        assert "feasibility" in result
        assert result["desired_macros"] == sample_desired_macros

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_execute_calls_api_methods(
        self, mock_fridge_api, mock_context, sample_fridge_contents, sample_desired_macros
    ):
        mock_fridge_api.get_available_macros.return_value = {
            "protein": 100.0,
            "carbs": 80.0,
            "fat": 40.0,
            "calories": 1000.0,
        }
        mock_fridge_api.get_ingredient_macros.return_value = {
            "protein": 31.0,
            "carbs": 0.0,
            "fat": 3.6,
            "calories": 165.0,
        }

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_select_ingredients",
            fridge_contents=sample_fridge_contents,
            desired_macros=sample_desired_macros,
        )

        operator.execute(mock_context)

        assert mock_fridge_api.get_available_macros.call_count >= 2
        assert mock_fridge_api.get_ingredient_macros.call_count >= len(
            sample_fridge_contents
        )

    def test_check_feasibility_all_feasible(self, sample_desired_macros):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_feasibility",
            fridge_contents=[],
            desired_macros=sample_desired_macros,
        )

        available_macros = {
            "protein": 100.0,
            "carbs": 80.0,
            "fat": 40.0,
            "calories": 800.0,
        }

        feasibility = operator._check_feasibility(available_macros)

        assert feasibility["protein"]["feasible"] is True
        assert feasibility["carbs"]["feasible"] is True
        assert feasibility["fat"]["feasible"] is True
        assert feasibility["calories"]["feasible"] is True
        assert feasibility["protein"]["surplus"] == 50.0
        assert feasibility["carbs"]["surplus"] == 40.0

    def test_check_feasibility_with_shortfall(self, sample_desired_macros):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_feasibility",
            fridge_contents=[],
            desired_macros=sample_desired_macros,
        )

        available_macros = {
            "protein": 30.0,
            "carbs": 80.0,
            "fat": 10.0,
            "calories": 400.0,
        }

        feasibility = operator._check_feasibility(available_macros)

        assert feasibility["protein"]["feasible"] is False
        assert feasibility["protein"]["shortfall"] == 20.0
        assert feasibility["fat"]["feasible"] is False
        assert feasibility["fat"]["shortfall"] == 10.0
        assert feasibility["carbs"]["feasible"] is True
        assert feasibility["calories"]["feasible"] is False

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_select_ingredients_returns_list(
        self, mock_fridge_api, sample_fridge_contents, sample_desired_macros
    ):
        mock_fridge_api.get_ingredient_macros.return_value = {
            "protein": 31.0,
            "carbs": 0.0,
            "fat": 3.6,
            "calories": 165.0,
        }

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_select",
            fridge_contents=sample_fridge_contents,
            desired_macros=sample_desired_macros,
        )

        selected = operator._select_ingredients()

        assert isinstance(selected, list)
        assert len(selected) >= 2
        assert len(selected) <= len(sample_fridge_contents)

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_select_ingredients_empty_fridge(self, mock_fridge_api):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_empty",
            fridge_contents=[],
            desired_macros={"protein": 50, "carbs": 40, "fat": 20, "calories": 500},
        )

        selected = operator._select_ingredients()

        assert selected == []

    def test_select_ingredients_zero_desired_macros(self, sample_fridge_contents):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_zero",
            fridge_contents=sample_fridge_contents,
            desired_macros={"protein": 0, "carbs": 0, "fat": 0, "calories": 0},
        )

        selected = operator._select_ingredients()

        assert selected == []

    def test_score_ingredient_perfect_match(self):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_score",
            fridge_contents=[],
            desired_macros={"protein": 30, "carbs": 50, "fat": 20, "calories": 500},
        )

        desired_ratios = {"protein": 0.3, "carbs": 0.5, "fat": 0.2}
        item_macros = {"protein": 30.0, "carbs": 50.0, "fat": 20.0, "calories": 500.0}

        score = operator._score_ingredient(item_macros, desired_ratios)

        assert score > 90

    def test_score_ingredient_poor_match(self):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_score",
            fridge_contents=[],
            desired_macros={"protein": 30, "carbs": 50, "fat": 20, "calories": 500},
        )

        desired_ratios = {"protein": 0.3, "carbs": 0.5, "fat": 0.2}
        item_macros = {"protein": 80.0, "carbs": 10.0, "fat": 10.0, "calories": 500.0}

        score = operator._score_ingredient(item_macros, desired_ratios)

        assert score < 50

    def test_score_ingredient_zero_macros(self):
        operator = SelectIngredientsForMacrosOperator(
            task_id="test_score",
            fridge_contents=[],
            desired_macros={"protein": 30, "carbs": 50, "fat": 20, "calories": 500},
        )

        desired_ratios = {"protein": 0.3, "carbs": 0.5, "fat": 0.2}
        item_macros = {"protein": 0.0, "carbs": 0.0, "fat": 0.0, "calories": 0.0}

        score = operator._score_ingredient(item_macros, desired_ratios)

        assert score == 0

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_select_ingredients_filters_error_responses(
        self, mock_fridge_api, sample_desired_macros
    ):
        fridge_contents = [
            {"type": "valid_ingredient", "units": "100g", "quantity": 1},
            {"type": "invalid_ingredient", "units": "100g", "quantity": 1},
        ]

        def mock_get_macros(ingredient_type, quantity):
            if ingredient_type == "invalid_ingredient":
                return {"error": "Ingredient not found"}
            return {"protein": 10.0, "carbs": 20.0, "fat": 5.0, "calories": 150.0}

        mock_fridge_api.get_ingredient_macros.side_effect = mock_get_macros

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_error_filter",
            fridge_contents=fridge_contents,
            desired_macros=sample_desired_macros,
        )

        selected = operator._select_ingredients()

        assert all(item["type"] != "invalid_ingredient" for item in selected)

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_select_ingredients_selects_75_percent(
        self, mock_fridge_api, sample_desired_macros
    ):
        fridge_contents = [
            {"type": f"ingredient_{i}", "units": "100g", "quantity": 1}
            for i in range(10)
        ]

        mock_fridge_api.get_ingredient_macros.return_value = {
            "protein": 10.0,
            "carbs": 20.0,
            "fat": 5.0,
            "calories": 150.0,
        }

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_percentage",
            fridge_contents=fridge_contents,
            desired_macros=sample_desired_macros,
        )

        selected = operator._select_ingredients()

        expected_count = max(2, int(len(fridge_contents) * 0.75))
        assert len(selected) == expected_count

    @patch("include.custom_operators.food_calculator.fridge_api")
    def test_execute_with_realistic_data(
        self, mock_fridge_api, mock_context
    ):
        fridge_contents = [
            {"type": "chicken breast", "units": "100g", "quantity": 2},
            {"type": "rice", "units": "100g", "quantity": 1.5},
            {"type": "broccoli", "units": "100g", "quantity": 1},
        ]

        desired_macros = {"protein": 60, "carbs": 50, "fat": 15, "calories": 550}

        def mock_get_available(items):
            return {
                "protein": 70.0,
                "carbs": 60.0,
                "fat": 20.0,
                "calories": 650.0,
            }

        def mock_get_ingredient(ingredient_type, quantity):
            macros_map = {
                "chicken breast": {"protein": 31.0, "carbs": 0.0, "fat": 3.6, "calories": 165.0},
                "rice": {"protein": 2.7, "carbs": 28.0, "fat": 0.3, "calories": 130.0},
                "broccoli": {"protein": 2.8, "carbs": 7.0, "fat": 0.4, "calories": 34.0},
            }
            base = macros_map.get(ingredient_type, {})
            return {
                "ingredient": ingredient_type,
                "quantity": quantity,
                "protein": base.get("protein", 0) * quantity,
                "carbs": base.get("carbs", 0) * quantity,
                "fat": base.get("fat", 0) * quantity,
                "calories": base.get("calories", 0) * quantity,
            }

        mock_fridge_api.get_available_macros.side_effect = mock_get_available
        mock_fridge_api.get_ingredient_macros.side_effect = mock_get_ingredient

        operator = SelectIngredientsForMacrosOperator(
            task_id="test_realistic",
            fridge_contents=fridge_contents,
            desired_macros=desired_macros,
        )

        result = operator.execute(mock_context)

        assert len(result["selected_ingredients"]) >= 2
        assert result["feasibility"]["protein"]["feasible"] is True
        assert result["feasibility"]["carbs"]["feasible"] is True

