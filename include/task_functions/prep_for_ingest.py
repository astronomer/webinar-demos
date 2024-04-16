import pandas as pd
from weaviate.util import generate_uuid5


def import_data(
    record,
    class_name: str,
):
    from airflow.operators.python import get_current_context

    df = pd.DataFrame(record, index=[0])

    df["uuid"] = df.apply(
        lambda x: generate_uuid5(identifier=x.to_dict(), namespace=class_name), axis=1
    )

    print(f"Passing {len(df)} objects for embedding and import.")

    print(df)

    context = get_current_context()
    context["custom_map_index"] = f"{df['uri'].values[0]} - Chunk: {df['chunk_index'].values[0]} / {df['chunks_per_doc'].values[0]}"

    return df.to_dict(orient="records")
