from airflow.policies import hookimpl
from airflow.exceptions import AirflowClusterPolicyViolation

# @hookimpl
# def task_policy(task):
#     from datetime import timedelta
    
#     if task.retries is None or task.retries == 0:
#         task.retries = 2
    
#     if task.retry_delay is None:
#         task.retry_delay = timedelta(minutes=1)
    

@hookimpl
def dag_policy(dag):
    """Ensure that Dag has at least one tag and skip all Dags tagged with `dev`."""
    if not dag.tags:
        raise AirflowClusterPolicyViolation(
            f"Dag {dag.dag_id} has no tags. At least one tag required. File path: {dag.fileloc}"
        )
    if "dev" in dag.tags:
        raise AirflowClusterPolicyViolation(
            f"Dag {dag.dag_id} has the `dev` tag. This Dag is only for development and should not be run in production. File path: {dag.fileloc}"
        )
    
    dag.tags.add("policy_compliant")

    if dag.dag_id == "test_policies_dag":
        dag.doc_md = "This Dag is only for testing the policies."

    dag.max_active_tasks = 10