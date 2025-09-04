"""
## Set complex dependencies between tasks and task groups

This DAG shows how to set different dependencies between tasks and task groups.
For the same pattern with TaskGroup see example_complex_dependencies_2.py.
"""

from airflow.sdk import dag, task_group
from datetime import datetime
from airflow.providers.standard.operators.empty import EmptyOperator


@dag
def complex_task_groups():
    top_level_task_0 = EmptyOperator(task_id="top_level_task_0")
    top_level_task_1 = EmptyOperator(task_id="top_level_task_1")
    top_level_task_2 = EmptyOperator(task_id="top_level_task_2")
    top_level_task_3 = EmptyOperator(task_id="top_level_task_3")

    @task_group
    def top_level_task_group_1():
        hello_task_1 = EmptyOperator(task_id="HELLO")
        nested_task_2_level_1 = EmptyOperator(task_id="nested_task_2_level_1")
        independent_task_in_tg = EmptyOperator(task_id="independent_task_in_tg")

        top_level_task_1 >> hello_task_1 >> nested_task_2_level_1
        top_level_task_2 >> nested_task_2_level_1
        nested_task_2_level_1 >> top_level_task_3

        return [hello_task_1, nested_task_2_level_1, independent_task_in_tg]

    @task_group
    def top_level_task_group_2():
        nested_task_3_level_1 = EmptyOperator(task_id="nested_task_3_level_1")

        @task_group
        def nested_task_group_1():
            hello_task_2 = EmptyOperator(task_id="HELLO")
            nested_task_4_level_1 = EmptyOperator(task_id="nested_task_4_level_1")

            top_level_task_1 >> hello_task_2 >> nested_task_4_level_1
            top_level_task_2 >> nested_task_4_level_1
            nested_task_4_level_1 >> top_level_task_3

            return [hello_task_2, nested_task_4_level_1]

        nested_task_group_1_objects = nested_task_group_1()

        nested_task_group_1_objects[0].task_group >> nested_task_3_level_1

        return nested_task_group_1_objects

    top_level_task_group_1_objects = top_level_task_group_1()
    top_level_task_group_2_objects = top_level_task_group_2()

    (
        top_level_task_0
        >> top_level_task_group_1_objects[0].task_group
        >> top_level_task_group_2_objects[0].task_group.parent_group
    )

    top_level_task_group_1_objects[0] >> top_level_task_group_2_objects[0]


complex_task_groups()