from airflow.sdk import dag, task, Param

@dag(
    params={
        "fact_topic": Param(
            type="string",
            default="yourself",
        ),
    },
)
def example_syntax_task_llm():

    @task.llm(
        model="gpt-4o-mini",
        result_type=str,
        system_prompt="Tell me a fun fact about the topic given.",
    )
    def llm_task(**context):
        fact_topic = context["params"]["fact_topic"]
        return fact_topic
    
    llm_task()

example_syntax_task_llm()