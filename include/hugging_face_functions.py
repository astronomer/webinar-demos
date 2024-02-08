def get_sentiment_score(
    model: str,
    text_input: str,
) -> float:
    """
    Get the sentiment score from a HuggingFace model.
    Args:
        model: The HuggingFace model to use.
        text_input: The input text.
    Returns:
        float: The sentiment score.
    """

    from transformers import pipeline

    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model=model,
    )

    result = sentiment_pipeline(text_input)
    sentiment_score = result[0]["score"]
    sentiment_label = result[0]["label"]

    return {"sentiment_score": sentiment_score, "sentiment_label": sentiment_label}
