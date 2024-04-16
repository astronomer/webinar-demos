import os
import pandas as pd


def extract_guides(folder_path, **context):
    files = [f for f in os.listdir(folder_path) if f.endswith(".md")]

    uris = []
    titles = []
    texts = []

    for file in files:
        file_path = os.path.join(folder_path, file)

        uris.append(file_path)
        titles.append(os.path.splitext(file)[1])
        timestamp = os.path.getmtime(file_path)

        with open(file_path, "r", encoding="utf-8") as f:
            texts.append(f.read())

    document_df = pd.DataFrame(
        {
            "uri": uris,
            "title": titles,
            "timestamp": timestamp,
            "full_text": texts,
            "type": "text/markdown",
        }
    )

    document_df['timestamp'] = document_df['timestamp'].astype(str)

    return document_df.to_dict(orient="records")


def extract_text_files(folder_path, **context):
    files = [f for f in os.listdir(folder_path) if f.endswith(".txt")]

    uris = []
    titles = []
    texts = []

    for file in files:
        file_path = os.path.join(folder_path, file)

        uris.append(file_path)
        titles.append(file.split(".")[0])
        timestamp = os.path.getmtime(file_path)

        with open(file_path, "r", encoding="utf-8") as f:
            texts.append(f.read())

    document_df = pd.DataFrame(
        {
            "uri": uris,
            "title": titles,
            "timestamp": timestamp,
            "full_text": texts,
            "type": "text/plain",
        }
    )

    return document_df.to_dict(orient="records")
