import logging
from datetime import timedelta

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

log = logging.getLogger(__name__)


def _log_callback_info(callback_name, context):
    ti = context["task_instance"]
    task_obj = context["task"]
    exception = context.get("exception")

    log.info("=== %s fired ===", callback_name)
    log.info("dag_id:     %s", ti.dag_id)
    log.info("task_id:    %s", ti.task_id)
    log.info("run_id:     %s", ti.run_id)
    log.info("map_index:  %s", ti.map_index)
    log.info("try_number: %s (retries configured: %s)", ti.try_number, task_obj.retries)

    if exception is None:
        log.info("failure reason: none reported in context")
    else:
        log.info(
            "failure reason: %s: %s", type(exception).__name__, exception
        )


def my_execute_callback_function(context):
    _log_callback_info("on_execute_callback", context)


def my_retry_callback_function(context):
    _log_callback_info("on_retry_callback", context)


def my_success_callback_function(context):
    _log_callback_info("on_success_callback", context)


def my_failure_callback_function(context):
    _log_callback_info("on_failure_callback", context)


def my_skipped_callback_function(context):
    _log_callback_info("on_skipped_callback", context)


@dag(
    default_args={
        "on_execute_callback": my_execute_callback_function,
        "on_retry_callback": my_retry_callback_function,
        "on_success_callback": my_success_callback_function,
        "on_failure_callback": my_failure_callback_function,
        "on_skipped_callback": my_skipped_callback_function,
    }
)
def task_level_callbacks_dag():
    """One task per callback path: success, retry then failure, and skip."""

    @task
    def succeeding_task():
        log.info("doing work that succeeds")

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def failing_task():
        raise ValueError("simulated failure: source API returned 503")

    @task
    def skipping_task():
        # on_skipped_callback fires only for AirflowSkipException raised inside the
        # task, not for tasks skipped by trigger rules or branching
        raise AirflowSkipException("simulated skip: no new files to process")

    succeeding_task()
    failing_task()
    skipping_task()


task_level_callbacks_dag()
