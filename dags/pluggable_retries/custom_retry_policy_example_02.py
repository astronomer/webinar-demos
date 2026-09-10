from datetime import timedelta

from airflow.sdk import (
    dag,
    task,
    RetryDecision,  # used in custom retry policies
    RetryPolicy,  # base class to write custom retry policies
)


class ChaoticRetryPolicy(RetryPolicy):

    def __init__(self, my_chance_of_failure=0.5) -> None:
        self.my_chance_of_failure = my_chance_of_failure

    def evaluate(self, exception, try_number, max_tries, context=None):
        import random

        my_num = random.random()
        print(f"Rolled: {my_num}")
        if my_num < self.my_chance_of_failure:
            return RetryDecision.fail(reason="Stop!")
        else:
            return RetryDecision.retry(reason="One more chance")


@dag(tags=["pluggable retries"])
def custom_retry_policy_example_02():

    @task(
        retries=5,
        retry_policy=ChaoticRetryPolicy(my_chance_of_failure=0.75),
        retry_delay=timedelta(seconds=2),
    )
    def chaos():
        raise Exception

    chaos()


custom_retry_policy_example_02()
