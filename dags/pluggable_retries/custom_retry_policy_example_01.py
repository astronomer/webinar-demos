from datetime import timedelta

from airflow.sdk import (
    dag,
    task,
    RetryDecision,  # used in custom retry policies
    RetryPolicy,  # base class to write custom retry policies
)


class NoRetryManualRunsRetryPolicy(RetryPolicy):

    def evaluate(self, exception, try_number, max_tries, context=None):
        if context and context["dag_run"].run_type == "manual":
            return RetryDecision.fail(reason="Not retrying on manual run")
        return RetryDecision.default()


@dag(tags=["pluggable retries"])
def custom_retry_policy_example_01():

    @task(
        retries=5,
        retry_policy=NoRetryManualRunsRetryPolicy(),
        retry_delay=timedelta(seconds=2),
    )
    def no_retry_manual_runs():
        raise Exception

    no_retry_manual_runs()


custom_retry_policy_example_01()
