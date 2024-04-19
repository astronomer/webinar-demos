from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.models.taskinstancekey import TaskInstanceKey
from include.custom_operators.gpt_fine_tune import OpenAIFineTuneOperator

XCOM_FINE_TUNE_JOB_ID = "fine_tune_job_id"

class OpenAIFineTuneJobLink(BaseOperatorLink):
    """
    Operator link for OpenAIFineTuneOperator.

    It allows users to access the fine-tuning job in the OpenAI UI.
    """
    name = "Fine-tune job OpenAI"

    operators = [OpenAIFineTuneOperator]

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        fine_tune_job_id = XCom.get_value(ti_key=ti_key, key=XCOM_FINE_TUNE_JOB_ID)
        return f"https://platform.openai.com/finetune/{fine_tune_job_id}"

class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        OpenAIFineTuneJobLink(),
    ]