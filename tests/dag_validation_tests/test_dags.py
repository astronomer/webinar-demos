import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


ALLOWED_OPERATORS = [
    "_PythonDecoratedOperator",
    "SelectIngredientsForMacrosOperator",
    "HITLEntryOperator",
    "HITLBranchOperator",
    "_AgentDecoratedOperator",
]


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_dag_uses_allowed_operators_only(dag_id, dag, fileloc):
    """
    Test that all Dags only use allowed operators.
    """
    for task in dag.tasks:
        assert any(
            task.task_type == allowed_op for allowed_op in ALLOWED_OPERATORS
        ), f"{task.task_id} in {dag_id} ({fileloc}) uses {task.task_type}, which is not in the list of allowed operators."


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_dag_has_tags(dag_id, dag, fileloc):
    """
    Test that all Dags have at least one tag.
    """
    assert dag.tags, f"Dag {dag_id} ({fileloc}) has no tags. All Dags must have at least one tag."


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_generate_recipe_dag_has_max_consecutive_failed_dag_runs_below_10(dag_id, dag, fileloc):
    """
    Test that the Dag with the Id "generate_recipe" has the parameter max_consecutive_failed_dag_runs set to a value less than 10.
    """
    if dag_id == "generate_recipe":
        assert dag.max_consecutive_failed_dag_runs, f"Dag {dag_id} ({fileloc}) has no max_consecutive_failed_dag_runs set. The recipe_generation Dag must have max_consecutive_failed_dag_runs set."
        assert dag.max_consecutive_failed_dag_runs < 10, f"Dag {dag_id} ({fileloc}) has max_consecutive_failed_dag_runs set to {dag.max_consecutive_failed_dag_runs}. The recipe_generation Dag must have max_consecutive_failed_dag_runs set to less than 10."


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_agent_tasks_have_execution_timeout(dag_id, dag, fileloc):
    """
    Test if all tasks using AgentDecoratedOperator have an execution_timeout set below 20 minutes.
    """
    MAX_TIMEOUT_SECONDS = 20 * 60

    for task in dag.tasks:
        if task.task_type == "_AgentDecoratedOperator":
            assert task.execution_timeout is not None, (
                f"Task {task.task_id} in Dag {dag_id} ({fileloc}) uses AgentDecoratedOperator "
                f"but has no execution_timeout set. All agent tasks must have an execution_timeout."
            )
            assert task.execution_timeout.total_seconds() <= MAX_TIMEOUT_SECONDS, (
                f"Task {task.task_id} in Dag {dag_id} ({fileloc}) has execution_timeout of "
                f"{task.execution_timeout.total_seconds()}s, which exceeds the maximum of {MAX_TIMEOUT_SECONDS}s (20 minutes)."
            )
