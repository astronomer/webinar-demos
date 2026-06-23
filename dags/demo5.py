from airflow.sdk import dag, task
from airflow.providers.common.ai.toolsets.sql import SQLToolset

@dag
def demo5():

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a data analyst",
        toolsets=[
            SQLToolset(db_conn_id="snowflake_astrotrips")
        ],
    )
    def analyze_planet_performance():
        return f"Analyze performance per planet"

    @task
    def print_report(report):
        print(report)

    _perfomance_analysis = analyze_planet_performance()
    print_report(_perfomance_analysis)

demo5()
