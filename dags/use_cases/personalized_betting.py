from airflow.sdk import dag, task, task_group, Asset, AssetWatcher, chain
from pendulum import datetime
import airflow_ai_sdk as ai_sdk
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
import os
from include.utils import get_info_for_high_value_users, post_message_ui
import json


# Define the SQS queue URL
SQS_QUEUE = os.getenv("SQS_QUEUE")

# Define a trigger that listens to an external message queue (AWS SQS in this case)
trigger = MessageQueueTrigger(
    aws_conn_id="aws_default",
    queue=SQS_QUEUE,
    waiter_delay=10,  # delay in seconds between polls
)

# Define an asset that watches for messages on the queue
sqs_queue_asset = Asset(
    "significant_user_event",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)],
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        assets=(sqs_queue_asset | Asset("user_360_gold")),
    ),  # once a day or whenever a significant user event appears in the message queue
    tags=["webinar"]
)
def personalized_betting():

    @task.branch
    def event_driven_vs_regular(**context):

        asset_event_name = None

        triggering_asset_events = context[
            "triggering_asset_events"
        ]  # fetch the triggering asset events
        for asset_event in triggering_asset_events[sqs_queue_asset]:
            asset_event_name = asset_event.asset.name
        if (
            asset_event_name == "significant_user_event"
        ):  # if the dag was triggered using event-driven scheduling only one personalized message is created
            print("Triggered by SQS queue asset...")
            return "event_driven.process_message"

        else:
            return "regular.get_user_data"

    _event_driven_vs_regular = event_driven_vs_regular()

    @task_group
    def event_driven():

        @task
        def process_message(**context):

            asset_event_name = None

            triggering_asset_events = context[
                "triggering_asset_events"
            ]  # fetch the triggering asset events
            for asset_event in triggering_asset_events[sqs_queue_asset]:
                asset_event_name = (
                    asset_event.asset.name
                )  # get the name of the triggering asset event
                asset_extra = (
                    asset_event.extra
                )  # get the extra information from the triggering asset event (when triggered via SQS this contains the message from the queue)

            if (
                asset_event_name == "significant_user_event"
            ):  # if the dag was triggered using event-driven scheduling only one personalized message is created
                print("Triggered by SQS queue asset...")
                print("Processing the message from the SQS queue: ", asset_extra)
                user_info = [
                    json.loads(asset_extra["payload"]["message_batch"][0]["Body"])
                ]

                return user_info

            else:
                return get_info_for_high_value_users()

        _process_message = process_message()

        @task_group
        def classify_user(user_info):

            @task
            def convert_json_to_string(user_info):
                return json.dumps(user_info)

            @task.llm_branch(
                model="gpt-4o-mini",
                system_prompt="Classify the user based on his most recent betting activity. ",
                allow_multiple_branches=False,  # only select one branch
            )
            def classify_user(user_info) -> str:
                return user_info

            @task
            def handle_winner(user_info):
                user_info[0]["classification"] = "winner"
                return user_info

            @task
            def handle_looser(user_info):
                user_info[0]["classification"] = "looser"
                return user_info

            @task
            def handle_high_risk(user_info):
                user_info[0]["classification"] = "high_risk"
                return user_info

            @task
            def handle_other(user_info):
                user_info[0]["classification"] = "other"
                return user_info

            @task(
                trigger_rule="all_done",
            )
            def get_updated_user_info(**context):
                updated_info_winner = context["ti"].xcom_pull(
                    task_ids="event_driven.classify_user.handle_winner",
                )

                updated_info_looser = context["ti"].xcom_pull(
                    task_ids="event_driven.classify_user.handle_looser",
                )
                updated_info_high_risk = context["ti"].xcom_pull(
                    task_ids="event_driven.classify_user.handle_high_risk",
                )
                updated_info_other = context["ti"].xcom_pull(
                    task_ids="event_driven.classify_user.handle_other",
                )

                # select the first non-empty list from the xcom pulls
                updated_info = (
                    updated_info_winner
                    or updated_info_looser
                    or updated_info_high_risk
                    or updated_info_other
                )

                return updated_info

            _convert_json_to_string = convert_json_to_string(user_info)
            result = classify_user(user_info=_convert_json_to_string)

            high_task = handle_winner(user_info)
            medium_task = handle_looser(user_info)
            low_task = handle_other(user_info)
            _high_risk = handle_high_risk(user_info)
            _get_updated_user_info = get_updated_user_info()

            chain(
                result,
                [high_task, medium_task, low_task, _high_risk],
                _get_updated_user_info,
            )

            return _get_updated_user_info

        chain(_event_driven_vs_regular, _process_message)
        _classify_user = classify_user(user_info=_process_message)
        return _classify_user

    _event_driven = event_driven()

    @task
    def fetch_classified_user_info(user_info):
        return user_info

    _fetch_classified_user_info = fetch_classified_user_info(user_info=_event_driven)

    @task_group
    def regular():
        @task
        def get_user_data():
            return get_info_for_high_value_users()

        _get_user_data = get_user_data()

    _regular = regular()

    @task(retries=0)
    def send_user_stats_to_compliance():
        # import random

        # if random.random() < 0.25:
        #     raise Exception(
        #         "Failed to send user stats to compliance. 500 Internal Server Error!"
        #     )
        pass

    _send_user_stats_to_compliance = send_user_stats_to_compliance()

    @task(
        trigger_rule="all_done",
    )
    def gather_user_info(**context):
        updated_info = context["ti"].xcom_pull(
            task_ids=[
                "regular.get_user_data",
                "fetch_classified_user_info",
            ]
        )
        # drop none values from the list
        updated_info = [info for info in updated_info if info is not None]
        print(updated_info)
        return updated_info[0]

    _gather_user_info = gather_user_info()

    chain(_gather_user_info, _send_user_stats_to_compliance)

    chain(
        _event_driven_vs_regular,
        [_fetch_classified_user_info, _regular],
        _gather_user_info,
    )

    @task_group
    def generate_personalized_message(user_info):

        @task(trigger_rule="all_done")
        def convert_json_to_string_gen_input(user_info):
            # This function converts the user info JSON to a string for LLM processing
            return json.dumps(user_info)

        _convert_json_to_string_gen_input = convert_json_to_string_gen_input(
            user_info=user_info
        )

        @task.llm(
            model="gpt-4o-mini",
            result_type=str,
            system_prompt=(
                "Create a personalized message for the user based on their betting activity ",
                "make sure to take their current classification into account.",
                "The sender is Betty from PlayToWin. Add plenty of emojis to make it more engaging.",
                "The message should be short and concise, but also friendly and engaging.",
                "The message should be in the user's preferred language.",
                "Make sure to suggest the next bet based on the user's preferences and betting history.",
            ),
        )
        def create_a_personalized_message(user_info) -> str:
            # This function transforms Airflow task input into LLM input
            return user_info

        _create_a_personalized_message = create_a_personalized_message(
            user_info=_convert_json_to_string_gen_input
        )

        @task(trigger_rule="all_done")
        def combine_user_info_with_message(user_info, message):

            user_info["personalized_message"] = message
            return user_info

        _combine_user_info_with_message = combine_user_info_with_message(
            user_info=user_info, message=_create_a_personalized_message
        )

        @task(trigger_rule="all_done")
        def send_personalized_message(user_info):
            post_message_ui(
                user_info=user_info,
                queue_url=SQS_QUEUE,
            )
            if user_info is type(list):
                user_info = user_info[0]
            print("Personalized message sent to user: ", user_info["user_id"])
            print("Message content: ", user_info["personalized_message"])

        _send_personalized_message = send_personalized_message(
            user_info=_combine_user_info_with_message
        )

        chain(_create_a_personalized_message, _combine_user_info_with_message)

    _generate_personalized_message = generate_personalized_message.expand(
        user_info=_gather_user_info
    )


personalized_betting()