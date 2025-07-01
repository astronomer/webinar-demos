from airflow.sdk import dag, task, Param

@dag(
    params={
        "sentence": Param(
            type="string",
            default="Airflow is ",
        ),
    },
)
def example_embed():

    @task
    def upstream_task(**context):
        return context["params"]["sentence"]

    @task.embed(
        model_name="BAAI/bge-small-en-v1.5",
    )
    def embed_sentence(sentence: str):
        sentence_to_embed = sentence + "awesome!"
        return sentence_to_embed

    @task
    def downstream_task(embeds: list):
        print(embeds)
        print("Load embeds to vector db!")

    _upstream_task = upstream_task()
    _embed_sentence = embed_sentence(
        sentence=_upstream_task
        )
    _downstream_task = downstream_task(
        embeds=_embed_sentence
    )




example_embed()
