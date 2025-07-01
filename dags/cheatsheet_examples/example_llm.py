from typing import Literal
from airflow.sdk import dag, task, Param
import airflow_ai_sdk as ai_sdk


class ProductFeedbackSummary(ai_sdk.BaseModel):
    summary: str
    sentiment: Literal["positive", "negative", "neutral"]
    feature_requests: list[str]


@dag(
    params={
        "statement": Param(
            type="string",
            default="I love Airflow "
            + "and use is for everything! "
            + "Can you add plot displays to the UI?",
        ),
    },
)
def example_llm():

    @task
    def upstream_task(**context):
        return context["params"]["statement"]

    @task.llm(
        model="gpt-4o-mini",
        system_prompt="Determine the"
         + "sentiment of the statement given.",
        result_type=ProductFeedbackSummary,  # Optional
    )
    def llm_task(**context):
        statement = context["params"]["statement"]
        return statement

    @task
    def downstream_task(llm_output: ProductFeedbackSummary):
        return llm_output["feature_requests"]

    _upstream_task = upstream_task()
    _llm_task = llm_task(statement=_upstream_task)
    _downstream_task = downstream_task(llm_output=_llm_task)


example_llm()
