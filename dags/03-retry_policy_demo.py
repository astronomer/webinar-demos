"""
LLM-powered retry classification, made visible on demand.
"""

from datetime import timedelta

from airflow.sdk import dag, get_current_context, task

_LLM_CONN_ID = "pydanticai_default"

try:
    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule

    _llm_policy = LLMRetryPolicy(
        llm_conn_id=_LLM_CONN_ID,
        timeout=30.0,
        fallback_rules=[
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )

    @dag(tags=["common-ai", "demo", "retry-policy"])
    def retry_policy_demo():

        @task(retries=3, retry_delay=timedelta(seconds=15), retry_policy=_llm_policy)
        def auth_error():
            """Classified as auth -> FAIL immediately (do not waste retries)."""
            raise PermissionError("403 Forbidden: API key expired for service account billing@astrotrips.io")

        @task(retries=3, retry_delay=timedelta(seconds=15), retry_policy=_llm_policy)
        def rate_limit_recovers():
            """Classified as rate_limit -> RETRY, then RECOVERS on the 3rd attempt.

            Fails on attempts 1-2 (rate limited), then succeeds. Shows the happy path:
            a transient error is classified as retryable, backed off, retried, and the
            task ends green -- not every retry ends in failure.
            """
            attempt = get_current_context()["ti"].try_number
            if attempt < 3:
                raise RuntimeError(
                    f"429 Too Many Requests (attempt {attempt}): rate limit exceeded, retry after 15 seconds."
                )
            return f"Recovered on attempt {attempt} after transient rate limiting."

        @task(retries=3, retry_delay=timedelta(seconds=15), retry_policy=_llm_policy)
        def data_error():
            """Classified as data -> FAIL immediately (retrying will not help)."""
            raise ValueError("Column 'passengers' expected INTEGER but got STRING in row 42.")

        auth_error()
        rate_limit_recovers()
        data_error()

    retry_policy_demo()

except ImportError:
    # RetryPolicy requires Airflow 3.3+; skip the DAG on older cores.
    pass
