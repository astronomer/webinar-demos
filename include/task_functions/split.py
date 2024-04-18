import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter


def split_text(records: list[dict]) -> list[dict]:
    """
    Split text into chunks.
    Args:
        records (list[dict]): A list of dictionaries containing text records.
    Returns:
        list[dict]: A list of dictionaries containing the split text records.
    """

    df = pd.DataFrame(records)

    splitter = RecursiveCharacterTextSplitter()

    df["chunks"] = df["full_text"].apply(
        lambda x: splitter.split_documents([Document(page_content=x)])
    )

    df = df.explode("chunks", ignore_index=True)
    df.dropna(subset=["chunks"], inplace=True)
    df["full_text"] = df["chunks"].apply(lambda x: x.page_content)

    for document in df["uri"].unique():
        chunks_per_doc = 0
        for i, chunk in enumerate(df.loc[df["uri"] == document, "chunks"]):
            print(f"Document: {document}, Chunk: {i}")
            df.loc[df["chunks"] == chunk, "chunk_index"] = i
            chunks_per_doc += 1
        df["chunks_per_doc"] = chunks_per_doc

    df["chunk_index"] = df["chunk_index"].astype(int)
    df["chunks_per_doc"] = df["chunks_per_doc"].astype(int)
    df.drop(["chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df.to_dict(orient="records")
