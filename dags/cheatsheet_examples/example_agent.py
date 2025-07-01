from airflow.sdk import dag, task, Param
from pydantic_ai import Agent


def get_current_weather(
    latitude: float,
    longitude: float,
) -> str:
    import requests

    URL = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}&"
        f"hourly=temperature_2m&"
        f"current=temperature_2m,"
        f"precipitation,relative_humidity_2m"
    )
    response = requests.get(URL)
    return response.json()


weather_report_agent = Agent(
    "gpt-4o-mini",
    system_prompt="""
    You should create a personalized weather 
    report for a user based on their location. 
    You can use the get_current_weather tool 
    to get the current weather based on a 
    latitude and longitude.
    """,
    tools=[
        get_current_weather,
    ],
)


@dag(
    params={
        "location": Param(
            type="string",
            default="New York",
        ),
    },
)
def example_agent():

    @task
    def upstream_task(**context):
        return context["params"]["location"]

    @task.agent(agent=weather_report_agent)
    def create_weather_report(location: str) -> str:
        return location

    @task
    def downstream_task(response: str):
        return {
            "response": response,
            "word_count": len(response.split()),
            "character_count": len(response),
        }

    _upstream_task = upstream_task()
    agent_response = create_weather_report(
        location=_upstream_task
    )
    downstream_task(agent_response)


example_agent()
