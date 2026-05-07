import pytest
from unittest.mock import patch
from include.fridge_api.fridge_api import MockFridgeAPI


class TestMockFridgeAPI:

    @pytest.fixture
    def fridge_api(self):
        return MockFridgeAPI()

    @pytest.fixture
    def sample_fridge_contents(self):
        return [
            {"type": "chicken breast", "units": "100g", "quantity": 2.0},
            {"type": "eggs", "units": "piece", "quantity": 3},
            {"type": "rice", "units": "100g", "quantity": 1.5},
        ]

    def test_get_available_macros_calculates_correctly(
        self, fridge_api, sample_fridge_contents
    ):
        result = fridge_api.get_available_macros(sample_fridge_contents)

        assert result["protein"] == 84.05
        assert result["carbs"] == 43.8
        assert result["fat"] == 22.65
        assert result["calories"] == 735

    def test_get_available_macros_with_empty_list(self, fridge_api):
        result = fridge_api.get_available_macros([])

        assert result["protein"] == 0.0
        assert result["carbs"] == 0.0
        assert result["fat"] == 0.0
        assert result["calories"] == 0.0

    def test_get_available_macros_rounds_to_two_decimals(self, fridge_api):
        fridge_contents = [
            {"type": "almonds", "units": "100g", "quantity": 0.333},
        ]

        result = fridge_api.get_available_macros(fridge_contents)

        assert result["protein"] == round(21.0 * 0.333, 2)
        assert result["carbs"] == round(22.0 * 0.333, 2)
        assert result["fat"] == round(50.0 * 0.333, 2)
        assert result["calories"] == round(579 * 0.333, 2)

        assert len(str(result["protein"]).split(".")[-1]) <= 2

    def test_get_ingredient_macros_with_default_quantity(self, fridge_api):
        result = fridge_api.get_ingredient_macros("strawberries")

        assert result["ingredient"] == "strawberries"
        assert result["quantity"] == 1.0
        assert result["unit"] == "100g"
        assert result["protein"] == 0.7
        assert result["carbs"] == 7.7
        assert result["fat"] == 0.3
        assert result["calories"] == 32

    def test_get_ingredient_macros_with_custom_quantity(self, fridge_api):
        result = fridge_api.get_ingredient_macros("eggs", quantity=5.0)

        assert result["ingredient"] == "eggs"
        assert result["quantity"] == 5.0
        assert result["unit"] == "piece"
        assert result["protein"] == 30.0
        assert result["carbs"] == 3.0
        assert result["fat"] == 25.0
        assert result["calories"] == 350

    def test_get_ingredient_macros_with_fractional_quantity(self, fridge_api):
        result = fridge_api.get_ingredient_macros("salmon", quantity=0.75)

        assert result["ingredient"] == "salmon"
        assert result["quantity"] == 0.75
        assert result["unit"] == "100g"
        assert result["protein"] == round(25.0 * 0.75, 2)
        assert result["carbs"] == round(0.0 * 0.75, 2)
        assert result["fat"] == round(13.0 * 0.75, 2)
        assert result["calories"] == round(208 * 0.75, 2)

    def test_get_ingredient_macros_returns_correct_structure(self, fridge_api):
        result = fridge_api.get_ingredient_macros("avocado", quantity=2.0)

        required_fields = [
            "ingredient",
            "quantity",
            "unit",
            "protein",
            "carbs",
            "fat",
            "calories",
        ]
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"


    def test_get_ingredient_macros_with_invalid_ingredient(self, fridge_api):
        with pytest.raises(ValueError, match="Ingredient dillithium not found"):
            fridge_api.get_ingredient_macros("dillithium")

    @patch("include.fridge_api.fridge_api.random.sample")
    @patch("include.fridge_api.fridge_api.random.randint")
    @patch("include.fridge_api.fridge_api.random.uniform")
    def test_fetch_fridge_contents_with_mocked_random(
        self, mock_uniform, mock_randint, mock_sample, fridge_api
    ):
        mock_sample.return_value = ["eggs", "milk", "cheese"]
        mock_randint.return_value = 3
        mock_uniform.return_value = 2.5

        result = fridge_api.fetch_fridge_contents(num_items=3)

        assert len(result) == 3

        for item in result:
            assert "type" in item
            assert "units" in item
            assert "quantity" in item

        eggs_item = next(item for item in result if item["type"] == "eggs")
        assert eggs_item["quantity"] == 3
        assert eggs_item["units"] == "piece"

        milk_item = next(item for item in result if item["type"] == "milk")
        assert milk_item["quantity"] == 2.5
        assert milk_item["units"] == "100ml"

        cheese_item = next(item for item in result if item["type"] == "cheese")
        assert cheese_item["quantity"] == 3
        assert cheese_item["units"] == "slice"

    def test_fetch_fridge_contents_respects_num_items(self, fridge_api):
        result = fridge_api.fetch_fridge_contents(num_items=5)
        assert len(result) == 5

        result = fridge_api.fetch_fridge_contents(num_items=10)
        assert len(result) == 10

    def test_fetch_fridge_contents_handles_excessive_num_items(self, fridge_api):
        total_foods_in_db = len(fridge_api.nutrition_db)
        result = fridge_api.fetch_fridge_contents(num_items=total_foods_in_db + 100)

        assert len(result) <= total_foods_in_db

    def test_fetch_fridge_contents_returns_valid_food_types(self, fridge_api):
        result = fridge_api.fetch_fridge_contents(num_items=20)

        for item in result:
            assert item["type"] in fridge_api.nutrition_db
            food_data = fridge_api.nutrition_db[item["type"]]
            assert item["units"] == food_data["unit"]

    def test_fetch_fridge_contents_quantities_are_positive(self, fridge_api):
        result = fridge_api.fetch_fridge_contents(num_items=15)

        for item in result:
            assert item["quantity"] > 0
            assert isinstance(item["quantity"], (int, float))

    def test_fetch_fridge_contents_default_fridge_id(self, fridge_api):
        result = fridge_api.fetch_fridge_contents()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_fetch_fridge_contents_custom_fridge_id(self, fridge_api):
        result = fridge_api.fetch_fridge_contents(fridge_id="custom_fridge_123")
        assert isinstance(result, list)

    def test_init_sets_base_url(self):
        api = MockFridgeAPI(base_url="https://custom.api.com")
        assert api.base_url == "https://custom.api.com"

    def test_init_default_base_url(self):
        api = MockFridgeAPI()
        assert api.base_url == "https://api.smartfridge.io"

    def test_init_loads_nutrition_database(self):
        api = MockFridgeAPI()
        assert api.nutrition_db is not None
        assert len(api.nutrition_db) > 0
        assert "chicken breast" in api.nutrition_db
        assert "eggs" in api.nutrition_db
        assert "rice" in api.nutrition_db

    def test_get_ingredient_macros_with_different_unit_types(self, fridge_api):
        piece_result = fridge_api.get_ingredient_macros("apple", quantity=2)
        assert piece_result["unit"] == "piece"

        gram_result = fridge_api.get_ingredient_macros("chicken breast", quantity=1.5)
        assert gram_result["unit"] == "100g"

        ml_result = fridge_api.get_ingredient_macros("milk", quantity=2)
        assert ml_result["unit"] == "100ml"

        slice_result = fridge_api.get_ingredient_macros("cheese", quantity=4)
        assert slice_result["unit"] == "slice"

    def test_get_available_macros_with_mixed_units(self, fridge_api):
        mixed_fridge = [
            {"type": "eggs", "units": "piece", "quantity": 2},
            {"type": "milk", "units": "100ml", "quantity": 3},
            {"type": "cheese", "units": "slice", "quantity": 4},
            {"type": "chicken breast", "units": "100g", "quantity": 1.5},
        ]

        result = fridge_api.get_available_macros(mixed_fridge)

        assert result["protein"] > 0
        assert result["calories"] > 0
        assert isinstance(result["protein"], float)
        assert isinstance(result["carbs"], float)
        assert isinstance(result["fat"], float)
        assert isinstance(result["calories"], (int, float))
