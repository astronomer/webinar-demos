"""
This DAG shows how to use the SDK to build an agent that writes blog posts for executives.
"""

from airflow.sdk import dag, task, ObjectStoragePath, Param
from bs4 import BeautifulSoup
from pydantic_ai import Agent
from pydantic_ai.common_tools.duckduckgo import duckduckgo_search_tool

import os

OBJECT_STORAGE_DST = os.getenv("OBJECT_STORAGE_DST")
CONN_ID_DST = os.getenv("CONN_ID_DST")
KEY_DST = os.getenv("KEY_DST")


async def get_technical_summary(url: str) -> str:
    """
    Get a technical summary of a page.
    """

    import requests

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    technial_research_agent = Agent(
        "gpt-4o-mini",
        system_prompt="""
        You are responsible for distilling information from a text. The summary will be used by a blog post writer to write a blog post.
        Focus on key information and keep it technical and concise.
        """,
    )

    return await technial_research_agent.run(soup.get_text())


blog_post_writer_agent = Agent(
    "o3-mini",
    system_prompt="""
    You are a blog post writer. You are given a topic and your job is to write a blog post about it.
    You can use the the `duckduckgo_search_tool` to search the web for relevant information.
    Always use the `get_technical_summary` tool to get a technical summary of a page once you have the url.
    Keep going until you have enough information to write a blog post. Assume you know nothing about the topic, so you need to search the web for relevant information.
    Do not use quotes in your search queries.
    Find at least 8-10 sources to include in the research report. If you run out of sources, keep searching the web for more information with variations of the question.
    Do not generate new information, only distill information from the web and uplevel it for executives. If you want to cite a source, make sure you fetch the full contents because the summary may not be enough.
    The target audience for your post are directors and executives, they care about ROI, cost savings, and efficiency.
    """,
    tools=[duckduckgo_search_tool(), get_technical_summary],
)


@task.agent(agent=blog_post_writer_agent)
def deep_research_task(**context) -> str:
    """
    This task performs a deep research on the given query.
    """
    query = context["params"]["query"]

    return query


@dag(
    params={
        "query": Param(
            type="string",
            default="Write a blog post about how the best orchestration and data engineering are the most important competitive advantage in the AI era",
        ),
    },
    tags=["webinar"],
)
def write_exec_blog_post():
    results = deep_research_task()

    @task
    def upload_results(results: str, **context):
        dag_run_id = context["ts_nodash"]
        base_dst = ObjectStoragePath(
            f"{OBJECT_STORAGE_DST}://{KEY_DST}/blog_posts", conn_id=CONN_ID_DST
        )
        file_path = base_dst / f"{dag_run_id}_blog_post.txt"

        file_path.touch()
        file_path.write_text(results)

        return results

    upload_results(results)


write_exec_blog_post()
