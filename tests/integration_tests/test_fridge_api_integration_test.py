import pytest
from include.fridge_api.fridge_api import fridge_api, MockFridgeAPI
from include.fridge_api.nutrition_data import NUTRITION_DATABASE


class TestFridgeAPIIntegration:

    def test_get_available_macros_with_real_data(self):
        fridge_contents = [
            {"type": "chicken breast", "quantity": 2},
            {"type": "rice", "quantity": 1},
            {"type": "broccoli", "quantity": 1.5},
        ]

        result = fridge_api.get_available_macros(fridge_contents)

        assert "protein" in result
        assert "carbs" in result
        assert "fat" in result
        assert "calories" in result
        assert result["protein"] > 0
        assert result["carbs"] > 0
        assert result["calories"] > 0

    def test_get_available_macros_calculates_correctly(self):
        fridge_contents = [
            {"type": "eggs", "quantity": 2},
        ]

        result = fridge_api.get_available_macros(fridge_contents)

        expected_protein = NUTRITION_DATABASE["eggs"]["protein"] * 2
        expected_carbs = NUTRITION_DATABASE["eggs"]["carbs"] * 2
        expected_fat = NUTRITION_DATABASE["eggs"]["fat"] * 2

        assert result["protein"] == expected_protein
        assert result["carbs"] == expected_carbs
        assert result["fat"] == expected_fat

    def test_get_available_macros_empty_list(self):
        result = fridge_api.get_available_macros([])

        assert result["protein"] == 0.0
        assert result["carbs"] == 0.0
        assert result["fat"] == 0.0
        assert result["calories"] == 0.0

    def test_get_available_macros_multiple_items(self):
        fridge_contents = [
            {"type": "milk", "quantity": 1},
            {"type": "banana", "quantity": 2},
            {"type": "oats", "quantity": 0.5},
        ]

        result = fridge_api.get_available_macros(fridge_contents)

        assert isinstance(result["protein"], float)
        assert isinstance(result["carbs"], float)
        assert result["protein"] > 0
        assert result["carbs"] > 0

    def test_get_ingredient_macros_single_quantity(self):
        result = fridge_api.get_ingredient_macros("chicken breast", 1)

        assert result["ingredient"] == "chicken breast"
        assert result["quantity"] == 1
        assert result["protein"] == NUTRITION_DATABASE["chicken breast"]["protein"]
        assert "unit" in result

    def test_get_ingredient_macros_multiple_quantity(self):
        result = fridge_api.get_ingredient_macros("rice", 2.5)

        assert result["quantity"] == 2.5
        assert result["protein"] == round(
            NUTRITION_DATABASE["rice"]["protein"] * 2.5, 2
        )
        assert result["carbs"] == round(NUTRITION_DATABASE["rice"]["carbs"] * 2.5, 2)

    def test_get_ingredient_macros_all_database_items(self):
        for ingredient in NUTRITION_DATABASE.keys():
            result = fridge_api.get_ingredient_macros(ingredient, 1)

            assert "protein" in result
            assert "carbs" in result
            assert "fat" in result
            assert "calories" in result
            assert "unit" in result

    def test_fetch_fridge_contents_returns_correct_structure(self):
        result = fridge_api.fetch_fridge_contents(num_items=5)

        assert isinstance(result, list)
        assert len(result) == 5

        for item in result:
            assert "type" in item
            assert "units" in item
            assert "quantity" in item
            assert item["type"] in NUTRITION_DATABASE

    def test_fetch_fridge_contents_default_num_items(self):
        result = fridge_api.fetch_fridge_contents()

        assert len(result) == 12

    def test_fetch_fridge_contents_quantity_ranges(self):
        result = fridge_api.fetch_fridge_contents(num_items=20)

        for item in result:
            unit = item["units"]
            quantity = item["quantity"]

            if unit == "piece":
                assert 1 <= quantity <= 5
                assert isinstance(quantity, int)
            elif unit == "slice":
                assert 1 <= quantity <= 8
                assert isinstance(quantity, int)
            elif "100g" in unit or "100ml" in unit:
                assert 0.5 <= quantity <= 5.0
            else:
                assert 0.5 <= quantity <= 3.0

    def test_fetch_fridge_contents_no_duplicates(self):
        result = fridge_api.fetch_fridge_contents(num_items=10)

        ingredient_types = [item["type"] for item in result]
        assert len(ingredient_types) == len(set(ingredient_types))

    def test_fetch_fridge_contents_respects_max_items(self):
        total_available = len(NUTRITION_DATABASE)
        result = fridge_api.fetch_fridge_contents(num_items=total_available + 10)

        assert len(result) <= total_available

    def test_multiple_api_instances_share_database(self):
        api1 = MockFridgeAPI()
        api2 = MockFridgeAPI()

        result1 = api1.get_ingredient_macros("chicken breast", 1)
        result2 = api2.get_ingredient_macros("chicken breast", 1)

        assert result1["protein"] == result2["protein"]
        assert result1["carbs"] == result2["carbs"]

    def test_nutrition_database_has_required_fields(self):
        required_fields = ["protein", "carbs", "fat", "calories", "unit"]

        for ingredient, data in NUTRITION_DATABASE.items():
            for field in required_fields:
                assert field in data, f"{ingredient} missing {field}"
                if field != "unit":
                    assert isinstance(
                        data[field], (int, float)
                    ), f"{ingredient}.{field} is not numeric"

    def test_nutrition_database_units_are_valid(self):
        valid_units = ["100g", "100ml", "piece", "slice"]

        for ingredient, data in NUTRITION_DATABASE.items():
            assert data["unit"] in valid_units, f"{ingredient} has invalid unit: {data['unit']}"

    def test_rounding_precision(self):
        result = fridge_api.get_ingredient_macros("chicken breast", 1.333)

        protein_str = str(result["protein"])
        if "." in protein_str:
            decimals = len(protein_str.split(".")[1])
            assert decimals <= 2

