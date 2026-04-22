from typing import Literal

from pydantic import BaseModel
from pydantic_ai.common_tools.duckduckgo import duckduckgo_search_tool

from airflow.sdk import dag, task, chain, Param


class PokedexEntry(BaseModel):
    species: str
    primary_type: Literal[
        "Fire", "Water", "Grass", "Electric", "Psychic", "Rock", "Ghost", "Normal"
    ]
    signature_move: str
    entry: str


@dag(
    params={"person_name": Param(type="string", default="Marc Lamberti")}
)
def task_agent_syntax_example():

    @task
    def upstream_task(**context) -> str:
        return context["params"]["person_name"]

    _upstream_task = upstream_task()

    @task.agent(
        llm_conn_id="pydanticai_default",  # Airflow conn for the LLM
        system_prompt="""
        You are a Pokedex. Given a person's name, use the web search tool
        to look up who they are, then invent a playful Pokedex entry for
        them as if they were a Pokemon: pick a species name, a primary
        type, a signature move, and a flavor-text entry in Pokedex style.
        """,
        output_type=PokedexEntry,  # pydantic schema for structured output
        agent_params={"tools": [duckduckgo_search_tool()]},  # web search
    )
    def generate_pokedex_entry(name: str):
        return name  # user prompt sent to the agent

    _generate_pokedex_entry = generate_pokedex_entry(name=_upstream_task)

    @task
    def print_result(entry: dict):
        import textwrap

        print(f"{entry['species']} - {entry['primary_type']}")
        print(f"Signature Move: {entry['signature_move']}")
        print("Entry:")
        for line in textwrap.wrap(entry["entry"], width=80):
            print(f"  {line}")

    _print_result = print_result(_generate_pokedex_entry)

    chain(_upstream_task, _generate_pokedex_entry, _print_result)


task_agent_syntax_example()
