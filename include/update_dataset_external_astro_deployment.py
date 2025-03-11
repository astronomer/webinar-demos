"""
Function to update a dataset in an Astro deployment.

For more information see: https://www.astronomer.io/docs/astro/best-practices/cross-deployment-dependencies/#datasets-example

"""


def update_dataset_external_astro_deployment(dataset_uri: str, extra: dict):

    import os
    import requests

    DEPLOYMENT_URL = os.getenv(
        "DEPLOYMENT_URL"
    )  # format: clq52ag32000108i8e3v3acml.astronomer.run/dz3uu847
    TOKEN = os.getenv(
        "TOKEN", "admin"
    )  # see: https://www.astronomer.io/docs/astro/deployment-api-tokens

    payload = {
        "dataset_uri": dataset_uri,
        "extra": extra,
    }
    response = requests.post(
        url=f"https://{DEPLOYMENT_URL}/api/v1/datasets/events",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )
    print(response.json())


if __name__ == "__main__":
    update_dataset_external_astro_deployment(
        dataset_uri="s3://critical-reports/canada", extra={"Hello": "I am a payload"}
    )
