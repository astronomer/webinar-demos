from airflow.sdk import dag, task, BaseOperator
import requests


_USERNAME = "admin"
_PASSWORD = "admin"
_HOST = "http://api-server:8080"  # To learn how to send API requests to Airflow running on Astro see: https://www.astronomer.io/docs/astro/airflow-api/


class MyCustomAirflowClientOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        import airflow_client.client
        from airflow_client.client.api.dag_api import DAGApi

        token_url = f"{_HOST}/auth/token"
        payload = {"username": _USERNAME, "password": _PASSWORD}
        headers = {"Content-Type": "application/json"}

        token_response = requests.post(token_url, json=payload, headers=headers)
        access_token = token_response.json().get("access_token")

        configuration = airflow_client.client.Configuration(
            host=_HOST, access_token=access_token
        )

        with airflow_client.client.ApiClient(configuration) as api_client:
            dag_api = DAGApi(api_client)
            dags = dag_api.get_dags(limit=10)
            print(dags)


@dag
def db_access_af3():
    my_task_1 = MyCustomAirflowClientOperator(
        task_id="my_task_1",
    )

    @task
    def use_api_directly():
        token_url = f"{_HOST}/auth/token"
        payload = {"username": _USERNAME, "password": _PASSWORD}
        headers = {"Content-Type": "application/json"}

        token_response = requests.post(token_url, json=payload, headers=headers)
        token = token_response.json().get("access_token")

        dags_url = f"{_HOST}/api/v2/dags"
        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        dags_response = requests.get(
            dags_url, headers=auth_headers, params={"limit": 10}
        )
        dags_data = dags_response.json()

        print(dags_data)

    use_api_directly()

    @task
    def use_the_airflow_client():
        import airflow_client.client
        from airflow_client.client.api.dag_api import DAGApi

        # On OSS Airflow you need to request a JWT token from the API
        # On Astro, you can use a DEPLOYMENT_API_TOKEN directly in the Configuration
        token_url = f"{_HOST}/auth/token"
        payload = {"username": _USERNAME, "password": _PASSWORD}
        headers = {"Content-Type": "application/json"}

        token_response = requests.post(token_url, json=payload, headers=headers)
        access_token = token_response.json().get("access_token")

        configuration = airflow_client.client.Configuration(
            host=_HOST, access_token=access_token  # On Astro, use a DEPLOYMENT_API_TOKEN
        )

        with airflow_client.client.ApiClient(configuration) as api_client:
            dag_api = DAGApi(api_client)
            dags = dag_api.get_dags(limit=10)
            print(dags)

    use_the_airflow_client()


db_access_af3() 
