import requests
import os
import json

def extract_local_files(source_unit):
    """
    Reads text from local files (.md, text).
    Args:
        source_unit (str): URI of a folder containing files.
    Returns:
        dict: Extracted text in the format
            {"source_unit": source_unit, "texts": [text1, text2, ...]}
    """

    uri = source_unit

    path = uri.split("file://")[1]

    files = os.listdir(path)
    files = [os.path.join(path, file) for file in files]

    info = {"source_unit": uri, "texts": []}
    for file in files:
        with open(file, "r") as f:
            text = f.read()

        info["texts"].append(text)

    return info


def extract_issues_gh_repo(source_unit, **context):
    """
    Extracts issues from a GitHub repository.
    Pulls the date of the last ingestion from the Airflow context.
    Needs a GitHub token to authenticate provided as an
    environment variable GH_TOKEN.
    Args:
        source_unit (str): GitHub repository name.
    Returns:
        dict: Extracted text in the format
            {"source_unit": source_unit, "texts": [text1, text2, ...]}
    """

    ingest_start_date = context["params"]["ingest_start_date"]
    repo_name = source_unit
    token = os.getenv("GH_TOKEN")

    url = f"https://api.github.com/repos/{repo_name}/issues?state=all&since={ingest_start_date}"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    r = requests.get(url, headers=headers)

    info = {"source_unit": repo_name, "texts": []}

    for issue in r.json():

        url = issue["url"]

        if "pulls" not in url:  # the endpoint can also fetch PRs we don't want those
            title = issue["title"]
            body = issue["body"]

            text = f"GitHub Issue Title: {title}, GitHub Issue Body: {body}"
            info["texts"].append(text)

    return info
