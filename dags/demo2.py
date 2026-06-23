from airflow.sdk import dag, task
from airflow.providers.http.operators.http import HttpOperator

@dag
def demo2():

    get_ditto = HttpOperator(
        task_id="get_ditto",
        http_conn_id="pokeapi",
        method="GET",
        endpoint="api/v2/pokemon/ditto",
        log_response=True,
    )

    @task
    def print_ditto(response: str):
        import json
        data = json.loads(response)
        print(f"Name: {data['name']}, base experience: {data['base_experience']}")

    print_ditto(get_ditto.output)

demo2()
