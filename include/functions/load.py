import json

def load_text_chunks_to_object_storage(info):

    source_unit_identifier = info["source_unit"].replace("/", "_")

    print(info)

    # save the texts in include/text_storage
    for i, text in enumerate(info["texts"]):
        with open(f"include/text_storage/{source_unit_identifier}_{i}.txt", "w") as f:
            f.write(text[0])

    return None
