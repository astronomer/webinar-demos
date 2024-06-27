from langchain.text_splitter import RecursiveCharacterTextSplitter
from include.functions.utils.ignore_text import REPLACEMENT_LIST  # TODO: Improve filtering of boilerplate language

def _replace_strings(text, replacements):
    """
    Replace strings in a single text based on replacement pairs.
    Args:
        text (str): The string to perform replacements on.
        replacements (list of tuple): List of (old, new) replacement pairs.
    Returns:
        str: The string after replacements.
    """
    for old, new in replacements:
        text = text.replace(old, new)
    return text


def transform_text(info):
    """
    Transform texts by applying specific replacements to each text.
    Args:
        texts (list of str): List of strings to transform.
    Returns:
        list of str: List of transformed strings.
    """
    info["texts"] = [_replace_strings(text, REPLACEMENT_LIST) for text in info["texts"]]

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=5000,
        chunk_overlap=100,
        length_function=len,
        separators=["\n\n", "\n", " ", ""]
    )

    info["texts"] = [text_splitter.split_text(text) for text in info["texts"]]

    return info
