"""Only runs in Airflow 3"""

from airflow.sdk import dag, task
import requests

@dag
def my_dag_using_the_rest_api():
    @task
    def access_metadata_via_rest_api():
        """
        GOOD PRACTICE: Using Airflow REST API to access metadata
        Airflow REST API documentation: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
        """
        import os

        USERNAME = "admin"
        PASSWORD = "admin"
        
        HOST = "http://upgrading-webinar_94fa33-api-server-1:8080/"  # UPDATE!!!
        print(f"HOST: {HOST}")
        
        # To learn how to send API requests to Airflow running on Astro see: 
        # https://www.astronomer.io/docs/astro/airflow-api/


        def get_jwt_token():
            """Get JWT token for API authentication"""
            token_url = f"{HOST}/auth/token"
            payload = {"username": USERNAME, "password": PASSWORD}
            headers = {"Content-Type": "application/json"}
            response = requests.post(token_url, json=payload, headers=headers)
            response.raise_for_status()
            print(f"Response: {response.json()}")
            token = response.json().get("access_token")
            return token

        def get_dag_runs(dag_id, token):
            """Get DAG runs using REST API"""
            url = f"{HOST}/api/v2/dags/{dag_id}/dagRuns"
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            print(f"Response: {response.json()}")
            return response.json()
        
        token = get_jwt_token()
        dag_id = "my_dag_using_the_rest_api"
        dag_runs = get_dag_runs(dag_id, token)

        print(f"Found {len(dag_runs)} dag runs")
        print(f"Dag runs: {dag_runs}")

    access_metadata_via_rest_api()


my_dag_using_the_rest_api()
