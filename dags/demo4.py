from airflow.sdk import dag, task

@dag
def demo4():

    @task
    def get_review():
        return "One of the most practical Airflow sessions I've attended"

    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Draft replies to webinar reviews",
    )
    def draft_reply(review):
        return f"Attendee review: {review}"

    @task
    def print_reply(reply):
        print(reply)

    _review = get_review()
    _reply = draft_reply(_review)
    print_reply(_reply)

demo4()
