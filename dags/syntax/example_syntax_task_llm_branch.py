from airflow.sdk import dag, task, Param, chain


@dag(
    params={
        "statement": Param(
            type="string",
            default="The sky is green",
        ),
    },
)
def example_syntax_task_llm_branch():

    @task.llm_branch(
        model="gpt-4o-mini",
        system_prompt="Choose a branch based on truthfulness of the statement given.",
        allow_multiple_branches=False,
    )
    def llm_branch_task(**context):
        statement = context["params"]["statement"]
        return statement

    @task
    def handle_true_statement(**context):
        statement = context["params"]["statement"]
        print(f"The followig statement is true")
        print(statement)

    @task
    def handle_false_statement(**context):
        statement = context["params"]["statement"]
        print(f"The followig statement is false")
        print(statement)

    @task
    def handle_other_statement(**context):
        statement = context["params"]["statement"]
        print(
            f"The followig statement could not be classified as clearly true or false"
        )
        print(statement)

    _llm_branch_task = llm_branch_task()

    chain(
        _llm_branch_task,
        [
            handle_true_statement(statement=_llm_branch_task),
            handle_false_statement(statement=_llm_branch_task),
            handle_other_statement(statement=_llm_branch_task),
        ],
    )


example_syntax_task_llm_branch()
