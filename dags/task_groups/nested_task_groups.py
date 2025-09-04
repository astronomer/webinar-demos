from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(tags=["webinar"])
def nested_task_groups():

    # this is also an example of acceptable top-level code!
    groups = []
    for g_id in range(1,3):
        @task_group(group_id=f"group{g_id}")
        def tg1():
            t1 = EmptyOperator(task_id="task1")
            t2 = EmptyOperator(task_id="task2")

            sub_groups = []
            for s_id in range(1,3):
                @task_group(group_id=f"sub_group{s_id}")
                def tg2():
                    st1 = EmptyOperator(task_id="task1")
                    st2 = EmptyOperator(task_id="task2")

                    st1 >> st2
                sub_groups.append(tg2())

            t1 >> sub_groups >> t2
        groups.append(tg1())

    groups[0] >> groups[1]

nested_task_groups()
