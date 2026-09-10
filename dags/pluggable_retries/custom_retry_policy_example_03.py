from datetime import timedelta

from airflow.sdk import RetryDecision, RetryPolicy, dag, task
from airflow.utils import timezone


class StaleRunRetryPolicy(RetryPolicy):
    """Stop retrying once the run is too stale to be useful."""

    STALENESS_THRESHOLD = timedelta(seconds=10)

    def evaluate(self, exception, try_number, max_tries, context=None):
        if context is None:
            return RetryDecision.default()
        logical_date = context["dag_run"].logical_date
        if timezone.utcnow() - logical_date > self.STALENESS_THRESHOLD:
            return RetryDecision.fail(
                reason="Run is past the staleness threshold, escalating"
            )
        return RetryDecision.default()
    

@dag(tags=["pluggable retries"])
def custom_retry_policy_example_03():

    @task(
        retries=10,
        retry_policy=StaleRunRetryPolicy(),
        retry_delay=timedelta(seconds=2),
    )
    def stop_retrying_if_stale():
        raise Exception
    
    
    stop_retrying_if_stale()


custom_retry_policy_example_03()