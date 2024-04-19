"""
Careful! This script will delete all queued dataset events in the Airflow database.
"""

import requests
from requests.auth import HTTPBasicAuth

USERNAME = "admin"
PASSWORD = "admin"
HOST = "http://localhost:8080"

# get all Dataset URIs
url = f"{HOST}/api/v1/datasets"

r_all_datasets = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))

dataset_uris = [dataset["uri"] for dataset in r_all_datasets.json()["datasets"]]

print(f"All dataset URIs: {dataset_uris}")


for dataset_uri in dataset_uris:

    # get queued dataset events for the dataset
    url = f"{HOST}/api/v1/datasets/queuedEvent/{dataset_uri}"
    r_queued_events_dataset = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))

    if r_queued_events_dataset.status_code == 404:
        print(f"No queued dataset events for {dataset_uri}. Skipping deletion.")
        pass
    elif r_queued_events_dataset.status_code != 200:
        print(
            f"Failed to get queued dataset events for {dataset_uri}. Error: {r_queued_events_dataset.text}"
        )
    else:

        # delete queued dataset events for the dataset
        url = f"{HOST}/api/v1/datasets/queuedEvent/{dataset_uri}"

        r_get_dataset_events = requests.delete(
            url, auth=HTTPBasicAuth(USERNAME, PASSWORD)
        )

        if r_get_dataset_events.status_code == 204:
            print(f"Dataset events for {dataset_uri} have been deleted.")
        else:
            print(
                f"Failed to delete dataset events for {dataset_uri}. Error: {r_get_dataset_events.text}"
            )