import requests
from requests.auth import HTTPBasicAuth

USERNAME = "admin"
PASSWORD = "admin"
HOST = "http://localhost:8080"
DATASET_1_URI = "file://include/examples/train_examples/ingested_examples/examples_long"
DATASET_2_URI = "file://include/examples/validation_examples/ingested_examples/examples_long"
EXTRA = {"type": "run_from_script"}

event_payload = {"dataset_uri": DATASET_1_URI, "extra": EXTRA}

url = f"{HOST}/api/v1/datasets/events"

response = requests.post(
    url, json=event_payload, auth=HTTPBasicAuth(USERNAME, PASSWORD)
)

print(response.json())

event_payload = {"dataset_uri": DATASET_2_URI, "extra": EXTRA}

url = f"{HOST}/api/v1/datasets/events"

response = requests.post(
    url, json=event_payload, auth=HTTPBasicAuth(USERNAME, PASSWORD)
)

print(response.json())
