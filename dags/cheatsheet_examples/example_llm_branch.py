from airflow.sdk import dag, task, Param, chain


@dag(
    params={
        "statement": Param(
            type="string",
            default="The sky is green",
        ),
    },
)
def example_llm_branch():

    @task
    def upstream_task(**context):
        return context["params"]["statement"]

    @task.llm_branch(
        model="gpt-4o-mini",
        system_prompt="Choose a branch based"
        + "on truthfulness of the statement given."
        + "If the truthfulness is unclear, "
        + "choose the unclear branch.",
        allow_multiple_branches=False,
    )
    def llm_branch_task(statement: str):
        return statement

    @task
    def handle_true_statement(**context):
        statement = context["params"]["statement"]
        print(f"The following statement is true:")
        print(statement)

    @task
    def handle_false_statement(**context):
        statement = context["params"]["statement"]
        print(f"The following statement is false")
        print(statement)

    @task
    def handle_unclear_statement(**context):
        statement = context["params"]["statement"]
        print(f"The following statement is unclear:")
        print(statement)

    _upstream_task = upstream_task()
    _llm_branch_task = llm_branch_task(statement=_upstream_task)

    chain(
        _llm_branch_task,
        [
            handle_true_statement(),
            handle_false_statement(),
            handle_unclear_statement(),
        ],
    )




example_llm_branch()
