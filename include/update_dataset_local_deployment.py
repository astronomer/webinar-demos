"""

Function to update a dataset in a local deployment of Apache Airflow run with the Astro CLI.

Endpoint used: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/create_dataset_event

"""


def update_dataset_local_deployment(dataset_uri: str, extra: dict):
    import requests
    from requests.auth import HTTPBasicAuth

    USERNAME = "admin"
    PASSWORD = "admin"
    HOST = "http://localhost:8080"

    event_payload = {"dataset_uri": dataset_uri, "extra": extra}

    url = f"{HOST}/api/v1/datasets/events"

    response = requests.post(
        url, json=event_payload, auth=HTTPBasicAuth(USERNAME, PASSWORD)
    )

    print(response.json())


if __name__ == "__main__":
    update_dataset_local_deployment(
        "s3://bucket/data.csv", {"Hello :)": "I am a payload"}
    )
