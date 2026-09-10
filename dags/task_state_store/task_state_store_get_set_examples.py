from datetime import timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import ResumableJobMixin
from airflow.sdk.bases.operator import BaseOperator


class TssContextExampleOperator(BaseOperator):

    def execute(self, context):
        my_num = context["task_state_store"].get("my_num")
        if my_num:
            self.log.info(f"Previously stored my_num: {my_num}")
        else:
            context["task_state_store"].set("my_num", 23)
            raise Exception("Simulated failure to trigger retry.")


class TssResumableExampleOperator(ResumableJobMixin, BaseOperator):

    external_id_key = "my_num"

    def execute(self, context):
        return self.execute_resumable(context)

    def submit_job(self, context):
        self.log.info("Submitting job, storing my_num=23 in the task_state_store.")
        return 23

    def get_job_status(self, external_id, context):
        return "succeeded"

    def is_job_active(self, status):
        return status == "running"

    def is_job_succeeded(self, status):
        return status == "succeeded"

    def poll_until_complete(self, external_id, context):
        if context["ti"].try_number == 1:
            raise Exception("Simulated failure to trigger retry.")

    def get_job_result(self, external_id, context):
        self.log.info(f"Previously stored my_num: {external_id}")
        return external_id


def _python_with_context(**context):
    my_num = context["task_state_store"].get("my_num")
    if my_num:
        print(f"Previously stored my_num: {my_num}")
    else:
        context["task_state_store"].set("my_num", 23)
        raise Exception("Simulated failure to trigger retry.")


@dag(tags=["task state store"])
def task_state_store_get_set_examples():

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def taskflow_with_context(**context):
        my_num = context["task_state_store"].get("my_num")
        if my_num:
            print(f"Previously stored my_num: {my_num}")
        else:
            context["task_state_store"].set("my_num", 23)
            raise Exception("Simulated failure to trigger retry.")

    taskflow_with_context()

    PythonOperator(
        task_id="python_operator_with_context",
        python_callable=_python_with_context,
        retries=1,
        retry_delay=timedelta(seconds=5),
    )

    TssContextExampleOperator(
        task_id="custom_op_with_context",
        retries=1,
        retry_delay=timedelta(seconds=5),
    )

    TssResumableExampleOperator(
        task_id="custom_op_with_resumable_mixin",
        retries=1,
        retry_delay=timedelta(seconds=5),
    )


task_state_store_get_set_examples()
