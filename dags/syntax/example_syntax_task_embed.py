from airflow.sdk import dag, task, Param
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


@dag(
    params={
        "search_term": Param(
            type="string",
            default="banana",
        ),
        "match_terms": Param(
            type="array",
            default=["sun", "chair", "lemon", "happiness", "red"],
        ),
    },
)
def example_syntax_task_embed():

    @task.embed(
        model_name="BAAI/bge-small-en-v1.5",
    )
    def embed_search_term(**context):
        search_term = context["params"]["search_term"]
        return search_term
    
    @task 
    def get_list_of_match_terms(**context):
        match_terms = context["params"]["match_terms"]
        return match_terms

    @task.embed(
        model_name="BAAI/bge-small-en-v1.5",
    )
    def embed_match_terms(match_term: str):
        return match_term

    @task
    def find_closest_match(
        embedded_search_term: list, embedded_match_terms: list, **context
    ):
        search_embedding = np.array(embedded_search_term).reshape(1, -1)
        match_embeddings = np.array(embedded_match_terms)

        similarities = cosine_similarity(search_embedding, match_embeddings)[0]

        closest_match_index = np.argmax(similarities)
        closest_similarity_score = similarities[closest_match_index]

        match_terms = context["params"]["match_terms"]
        closest_match_term = match_terms[closest_match_index]

        # print the scores for all the match terms
        for i, score in enumerate(similarities):
            print(f"Similarity score for {context['params']['search_term']} and {match_terms[i]}: {score}")

        return {
            "closest_match": closest_match_term,
            "similarity_score": float(closest_similarity_score),
        }

    _get_list_of_match_terms = get_list_of_match_terms()
    _search_embeddings = embed_search_term()
    _match_embeddings = embed_match_terms.expand(match_term=_get_list_of_match_terms)

    find_closest_match(_search_embeddings, _match_embeddings)


example_syntax_task_embed()
