import unittest
from unittest.mock import patch, MagicMock
from include.hugging_face_functions import get_sentiment_score


class TestGetSentimentScore(unittest.TestCase):

    @patch("transformers.pipeline")
    def test_positive_sentiment(self, mock_pipeline):

        mock_pipeline.return_value = MagicMock(
            return_value=[{"label": "positive", "score": 0.95}]
        )

        test_model = "distilbert-base-uncased-finetuned-sst-2-english"
        test_text_input = "I love this product!"

        sentiment = get_sentiment_score(test_model, test_text_input)

        self.assertEqual(sentiment["sentiment_label"], "positive")
        self.assertEqual(sentiment["sentiment_score"], 0.95)

    @patch("transformers.pipeline")
    def test_negative_sentiment(self, mock_pipeline):

        mock_pipeline.return_value = MagicMock(
            return_value=[{"label": "negative", "score": 0.95}]
        )

        test_model = "distilbert-base-uncased-finetuned-sst-2-english"
        test_text_input = "I hate this product!"

        sentiment = get_sentiment_score(test_model, test_text_input)

        self.assertEqual(sentiment["sentiment_label"], "negative")
        self.assertEqual(sentiment["sentiment_score"], 0.95)


if __name__ == "__main__":
    unittest.main()
