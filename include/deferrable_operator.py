import asyncio
import time
from asgiref.sync import sync_to_async
from typing import Any, Sequence, AsyncIterator
from airflow.configuration import conf
from airflow.models.baseoperator import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from airflow.datasets import Dataset


class MyTrigger(BaseTrigger):
    """
    This is an example of a custom trigger that waits for a binary random choice
    between 0 and 1 to be 1.
    Args:
        poll_interval (int): How many seconds to wait between async polls.
        my_kwarg_passed_into_the_trigger (str): A kwarg that is passed into the trigger.
    Returns:
        my_kwarg_passed_out_of_the_trigger (str): A kwarg that is passed out of the trigger.
    """

    def __init__(
        self,
        poll_interval: int = 60,
        my_kwarg_passed_into_the_trigger: str = "notset",
        my_kwarg_passed_out_of_the_trigger: str = "notset",
        # you can add more arguments here
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.my_kwarg_passed_into_the_trigger = my_kwarg_passed_into_the_trigger
        self.my_kwarg_passed_out_of_the_trigger = my_kwarg_passed_out_of_the_trigger

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize MyTrigger arguments and classpath.
        All arguments must be JSON serializable.
        This will be returned by the trigger when it is complete and passed as `event` to the
        `execute_complete` method of the deferrable operator.
        """

        return (
            "include.deferrable_operator.MyTrigger",  # this is the classpath for the Trigger
            {
                "poll_interval": self.poll_interval,
                "my_kwarg_passed_out_of_the_trigger": self.my_kwarg_passed_out_of_the_trigger,
                # you can add more kwargs here
            },
        )

    # The run method is an async generator that yields TriggerEvents when the desired condition is met
    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            result = (
                await self.my_trigger_function()
            )  # The my_trigger_function is awaited and where the condition is checked
            if result == 1:
                self.log.info(f"Result was 1, thats the number! Triggering event.")

                self.log.info(
                    f"Kwarg passed in was: {self.my_kwarg_passed_into_the_trigger}"
                )
                # This is how you pass data out of the trigger, by setting attributes that get serialized
                self.my_kwarg_passed_out_of_the_trigger = "apple"
                self.log.info(
                    f"Kwarg to be passed out is: {self.my_kwarg_passed_out_of_the_trigger}"
                )
                # Fire the trigger event! This gets a worker to execute the operator's `execute_complete` method
                yield TriggerEvent(self.serialize())
                return  # The return statement prevents the trigger from running again
            else:
                self.log.info(
                    f"Result was not the one we are waiting for. Sleeping for {self.poll_interval} seconds."
                )
                # If the condition is not met, the trigger sleeps for the poll_interval
                # this code can run multiple times until the condition is met
                await asyncio.sleep(self.poll_interval)

    # This is the function that is awaited in the run method
    @sync_to_async
    def my_trigger_function(self) -> str:
        """
        This is where what you are waiting for goes For example a call to an
        API to check for the state of a cloud resource.
        This code can run multiple times until the condition is met.
        """

        import random

        randint = random.choice([0, 1])
        self.log.info(f"Random number: {randint}")

        return randint


class MyOperator(BaseOperator):
    """
    Deferrable operator that waits for a binary random choice between 0 and 1 to be 1.
    Args:
        wait_for_completion (bool): Whether to wait for the trigger to complete.
        poke_interval (int): How many seconds to wait between polls,
            both in deferrable or sensor mode.
        deferrable (bool): Whether to defer the operator. If set to False,
            the operator will act as a sensor.
    Returns:
        str: A kwarg that is passed through the trigger and returned by the operator.
    """

    template_fields: Sequence[str] = (
        "wait_for_completion",
        "poke_interval",
    )
    ui_color = "#73deff"

    def __init__(
        self,
        *,
        # you can add more arguments here
        my_alias_name: str = "default_alias",
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),  # this default is a convention to be able to set the operator to deferrable in the config
        # using AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE=True
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.my_alias_name = my_alias_name
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self._defer = deferrable

    def execute(self, context: Context):

        # Add code you want to be executed before the deferred part here (this code only runs once)

        # turns operator into sensor/deferred operator
        if self.wait_for_completion:
            # Starting the deferral process
            if self._defer:
                self.log.info(
                    "Operator in deferrable mode. Starting the deferral process."
                )
                self.defer(
                    trigger=MyTrigger(
                        poll_interval=self.poke_interval,
                        my_kwarg_passed_into_the_trigger="lemon",
                        # you can pass information into the trigger here
                    ),
                    method_name="execute_complete",
                    kwargs={"kwarg_passed_to_execute_complete": "tomato"},
                    # kwargs get passed through to the execute_complete method
                )
            else:  # regular sensor part
                while True:
                    self.log.info("Operator in sensor mode. Polling.")
                    time.sleep(self.poke_interval)
                    import random

                    # This is where you would check for the condition you are waiting for
                    # when using the operator as a regular sensor
                    # This code can run multiple times until the condition is met

                    randint = random.choice([0, 1])

                    self.log.info(f"Random number: {randint}")
                    if randint == 1:
                        self.log.info("Result was 1, thats the number! Continuing.")
                        return randint
                    self.log.info(
                        "Result was not the one we are waiting for. Sleeping."
                    )
        else:
            self.log.info("Not waiting for completion.")

        # Add code you want to be executed after the deferred part here (this code only runs once)
        # you can have as many deferred parts as you want in an operator

    def execute_complete(
        self,
        context: Context,
        event: tuple[str, dict[str, Any]],
        kwarg_passed_to_execute_complete: str,  # make sure to add the kwargs you want to pass through
    ):
        """Execute when the trigger is complete. This code only runs once."""

        self.log.info("Trigger is complete.")
        self.log.info(f"Event: {event}")  # printing the serialized event

        message_from_the_trigger = event[1]["my_kwarg_passed_out_of_the_trigger"]

        # you can push additional data to XCom here
        context["ti"].xcom_push("message_from_the_trigger", message_from_the_trigger)

        # you can ONLY attach events to aliases in the execute_complete method
        context["outlet_events"][self.my_alias_name].add(
            Dataset(f"x-{message_from_the_trigger}")
        )

        return kwarg_passed_to_execute_complete  # the returned value gets pushed to XCom as `return_value`
