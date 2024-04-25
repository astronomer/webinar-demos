#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import time
from asgiref.sync import sync_to_async
from typing import TYPE_CHECKING, Any, Sequence, cast, AsyncIterator

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.triggers.base import BaseTrigger, TriggerEvent
from pendulum import from_timestamp

from openai import OpenAI

XCOM_FINE_TUNE_JOB_ID = "fine_tune_job_id"
XCOM_FINE_TUNE_MODEL_NAME = "fine_tune_model"

if TYPE_CHECKING:

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class OpenAIFineTuneJobLink(BaseOperatorLink):
    """
    Operator link for OpenAIFineTuneOperator.

    It allows users to access the fine-tuning job in the OpenAI UI.
    """

    name = "Fine-tune job OpenAI"

    def get_link(self, *, ti_key: TaskInstanceKey) -> str:
        fine_tune_job_id = XCom.get_value(ti_key=ti_key, key=XCOM_FINE_TUNE_JOB_ID)
        return f"https://platform.openai.com/finetune/{fine_tune_job_id}"


class OpenAIFineTuneTrigger(BaseTrigger):
    """
    Waits asynchronously for a Fine-tuning job on OpenAI to complete.

    :param fine_tune_job_id: The ID of the fine-tuning job.
    :openai_api_key: The OpenAI API key.
        If not provided, it will be fetched from the environment variable OPENAI_API_KEY.
    :poll_interval: The interval at which to poll the OpenAI API for the job status.
    """

    def __init__(
        self,
        fine_tune_job_id: str,
        openai_api_key: str = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.fine_tune_job_id = fine_tune_job_id
        self.openai_api_key = openai_api_key
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize OpenAIFineTuneTrigger arguments and classpath."""
        return (
            "include.custom_operators.gpt_fine_tune.OpenAIFineTuneTrigger",
            {
                "fine_tune_job_id": self.fine_tune_job_id,
                "openai_api_key": self.openai_api_key,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Check periodically if the dag run exists, and has hit one of the states yet, or not."""
        while True:
            fine_tune_info = await self.get_fine_tune_job_info()
            if fine_tune_info.status == "succeeded":
                self.log.info("Fine-tuning job completed successfully.")
                yield TriggerEvent(self.serialize())
                return
            if fine_tune_info.status == "failed":
                self.log.error("Fine-tuning job failed.")
                yield TriggerEvent(self.serialize())
                return
            if fine_tune_info.status == "cancelled":
                self.log.info("Fine-tuning job was cancelled.")
                yield TriggerEvent(self.serialize())
                return
            self.log.info(
                f"Fine-tuning job status: {fine_tune_info.status}. Trigger sleeping for {self.poll_interval} seconds."
            )
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    def get_fine_tune_job_info(self) -> str:
        """Get the status of the fine-tuning job."""

        if self.openai_api_key:
            client = OpenAI(api_key=self.openai_api_key)
        else:
            client = (
                OpenAI()
            )  # if no key is provided, attempt to fetch from the env OPEN_AI_API_KEY

        fine_tune_info = client.fine_tuning.jobs.retrieve(self.fine_tune_job_id)

        return fine_tune_info


class OpenAIFineTuneOperator(BaseOperator):
    """
    Fine tunes a model on OpenAI.

    :param fine_tuning_file_id: The ID of training examples file for fine-tuning.
    :param validation_file_id: The ID of the validation examples file.
    :param openai_api_key: The OpenAI API key.
        If not provided, it will be fetched from the environment variable OPENAI_API_KEY.
    :param model: The model to fine-tune (default: gpt-3.5-turbo).
    :param suffix: A suffix to append to the fine-tuned model name.
        If not provided, the logical date timestamp will be used. (default: None)
    :param wait_for_completion: Whether to wait for the fine-tuning job to complete. (default: False)
    :param poke_interval: The interval at which to poll the OpenAI API for the job status. (default: 60)
    :param deferrable: Whether to use deferrable mode. (default: False)
    """

    template_fields: Sequence[str] = (
        "fine_tuning_file_id",
        "validation_file_id",
        "openai_api_key",
        "model",
        "suffix",
        "wait_for_completion",
        "poke_interval",
    )
    template_fields_renderers = {"conf": "py"}
    ui_color = "#73deff"
    operator_extra_links = [OpenAIFineTuneJobLink()]

    def __init__(
        self,
        *,
        fine_tuning_file_id: str,
        validation_file_id: str,
        openai_api_key: str = None,
        model: str = "gpt-3.5-turbo",
        suffix: str = None,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.fine_tuning_file_id = fine_tuning_file_id
        self.validation_file_id = validation_file_id
        self.openai_api_key = openai_api_key
        self.model = model
        self.suffix = suffix
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self._defer = deferrable

    def execute(self, context: Context):

        if self.openai_api_key:
            client = OpenAI(api_key=self.openai_api_key)
        else:
            client = (
                OpenAI()
            )  # fetches the API key from the environment var OPENAI_API_KEY

        if self.suffix:
            self.suffix = self.suffix
        else:
            self.suffix = context[
                "ts_nodash"
            ]  # use the logical date timestamp as suffix

        fine_tune_info = client.fine_tuning.jobs.create(
            training_file=self.fine_tuning_file_id,
            validation_file=self.validation_file_id,
            model=self.model,
            suffix=self.suffix,
        )

        self.fine_tune_job_id = fine_tune_info.id
        self.fine_tune_model_name = fine_tune_info.fine_tuned_model

        self.log.info(f"Fine-tuning job created. Job ID: {self.fine_tune_job_id}")

        # Store the fine-tune job ID in XCom
        ti = context["task_instance"]
        ti.xcom_push(key=XCOM_FINE_TUNE_JOB_ID, value=self.fine_tune_job_id)

        if self.wait_for_completion:
            # Kick off the deferral process
            if self._defer:
                self.log.info(
                    f"Deferring the task of checking for the status of Fine-tuning job: {self.fine_tune_job_id}"
                )
                self.defer(
                    trigger=OpenAIFineTuneTrigger(
                        fine_tune_job_id=self.fine_tune_job_id,
                        openai_api_key=self.openai_api_key,
                        poll_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )
            # wait for fine-tuning job to complete
            while True:
                self.log.info(
                    f"Waiting for fine-tuning job {self.fine_tune_job_id} to complete. Sleeping for {self.poke_interval} seconds."
                )
                time.sleep(self.poke_interval)

                result_files = self.get_fine_tune_job_info(context)

                if result_files:
                    return result_files

        self.log.info(
            f"Fine-tuning job {self.fine_tune_job_id} was kicked off. Not waiting for completion."
            + "Set wait_for_completion=True to wait for completion."
        )

    def execute_complete(self, context: Context, event: tuple[str, dict[str, Any]]):
        """Execute when the trigger is complete."""

        self.log.info("Trigger is complete.")
        self.log.info(f"Event: {event}")
        fine_tune_job_id = event[1]["fine_tune_job_id"]
        print(fine_tune_job_id)
        result_files = self.get_fine_tune_job_info(
            fine_tune_job_id=fine_tune_job_id, context=context
        )

        return result_files

    def get_fine_tune_job_info(self, fine_tune_job_id, context: Context) -> str:

        if self.openai_api_key:
            client = OpenAI(api_key=self.openai_api_key)
        else:
            client = (
                OpenAI()
            )  # if no key is provided, attempt to fetch from the env OPEN_AI_API_KEY

        fine_tune_info = client.fine_tuning.jobs.retrieve(fine_tune_job_id)

        self.log.info(f"Checked status of fine-tuning job {fine_tune_job_id}")

        if fine_tune_info.status == "queued":
            self.log.info(f"Fine-tuning job {fine_tune_job_id} is queued.")
        if fine_tune_info.status == "validating_files":
            self.log.info(f"Fine-tuning job {fine_tune_job_id} is validating files.")
        if fine_tune_info.status == "running":
            self.log.info(f"Fine-tuning job {fine_tune_job_id} is running.")
        if fine_tune_info.status == "succeeded":
            self.log_fine_tune_info(fine_tune_info, context)
            return fine_tune_info.result_files
        if fine_tune_info.status == "failed":
            raise AirflowException(
                f"Fine-tuning job {fine_tune_job_id} failed: "
                + f"Status: {fine_tune_info.error.code}"
                + f"{fine_tune_info.error.message}"
            )
        if fine_tune_info.status == "cancelled":
            raise AirflowException(f"Fine-tuning job {fine_tune_job_id} was cancelled.")

        AirflowException(
            f"Fine-tuning job {fine_tune_job_id} in unknown state {fine_tune_info.status}"
        )

    def log_fine_tune_info(self, fine_tune_info, context: Context):

        # compute duration
        fine_tuning_duration_seconds = (
            fine_tune_info.finished_at - fine_tune_info.created_at
        )
        fine_tuning_duration_hr = fine_tuning_duration_seconds // 3600
        fine_tuning_duration_min = (fine_tuning_duration_seconds % 3600) // 60
        fine_tuning_duration_s = (fine_tuning_duration_seconds % 3600) % 60

        # log fine tuning info
        self.log.info("Fine-tuning job completed successfully.")
        self.log.info("--- Fine-tuning Job Info ---")
        self.log.info(f"Fine-tuned model: {fine_tune_info.fine_tuned_model}")
        self.log.info(f"Model: {fine_tune_info.model}")
        self.log.info(f"Model ID: {fine_tune_info.id}")
        self.log.info(f"Organization ID: {fine_tune_info.organization_id}")
        self.log.info(f"Trained tokens: {fine_tune_info.trained_tokens}")
        self.log.info(f"Training file: {fine_tune_info.training_file}")
        self.log.info(
            f"Hyperparameters: {fine_tune_info.hyperparameters.n_epochs} epochs, "
            + f"batch size: {fine_tune_info.hyperparameters.batch_size}, "
            + f"learning rate multiplier: {fine_tune_info.hyperparameters.learning_rate_multiplier}"
        )
        self.log.info(f"Result file ids: {fine_tune_info.result_files}")

        self.log.info("--- Fine tuning Duration ---")
        self.log.info(f"Created at: {from_timestamp(fine_tune_info.created_at)}")
        self.log.info(f"Finished at: {from_timestamp(fine_tune_info.finished_at)}")
        self.log.info(
            f"Fine-tuning Duration: {fine_tuning_duration_hr} hr {fine_tuning_duration_min} min {fine_tuning_duration_s} s"
        )

        print(fine_tune_info.fine_tuned_model)

        context["ti"].xcom_push(XCOM_FINE_TUNE_MODEL_NAME, fine_tune_info.fine_tuned_model)
