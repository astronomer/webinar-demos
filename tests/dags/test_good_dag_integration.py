from include.hugging_face_functions import get_sentiment_score


def test_get_sentiment_score_returns_positive_for_positive_feedback_roberta():
    """
    Integration test to verify that get_sentiment_score correctly returns a
    positive sentiment score for positive feedback using the
    ardiffnlp/twitter-roberta-base-sentiment-latest model.
    """
    test_model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    test_text_input = "I love this product!"

    sentiment = get_sentiment_score(test_model, test_text_input)
    sentiment_label = sentiment["sentiment_label"]

    assert (
        sentiment_label == "positive"
    ), "The sentiment label for clear positive feedback should be positive."


def test_get_sentiment_score_returns_negative_for_negative_feedback_roberta():
    """
    Integration test to verify that get_sentiment_score correctly returns a
    negative sentiment score for negative feedback using the
    cardiffnlp/twitter-roberta-base-sentiment-latest model.
    """
    test_model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    test_text_input = "I hate this product!"

    sentiment = get_sentiment_score(test_model, test_text_input)
    sentiment_label = sentiment["sentiment_label"]

    assert (
        sentiment_label == "negative"
    ), "The sentiment label for clear negative feedback should be negative."


def test_get_sentiment_score_is_reasonably_sure_for_clearly_positive_feedback_roberta():
    """
    Integration test to verify that get_sentiment_score correctly returns a
    high sentiment score for positive feedback using the
    cardiffnlp/twitter-roberta-base-sentiment-latest model.
    """
    test_model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    test_text_input = "I love this product!"

    sentiment = get_sentiment_score(test_model, test_text_input)
    sentiment_score = sentiment["sentiment_score"]

    assert (
        sentiment_score > 0.9
    ), "The sentiment score for clear positive feedback should be high."


def test_get_sentiment_score_is_reasonably_sure_for_clearly_negative_feedback_roberta():
    """
    Integration test to verify that get_sentiment_score correctly returns a
    low sentiment score for negative feedback using the
    cardiffnlp/twitter-roberta-base-sentiment-latest model.
    """
    test_model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    test_text_input = "I hate this product!"

    sentiment = get_sentiment_score(test_model, test_text_input)
    sentiment_score = sentiment["sentiment_score"]

    assert (
        sentiment_score > 0.9
    ), "The sentiment score for clear negative feedback should be low."
