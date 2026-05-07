import pytest
from unittest.mock import patch, MagicMock
from pendulum import datetime


class TestMyDagTaskFlow:

    def test_dag_loads(self):
        from dags.simple_example_dag_test.my_dag_task_flow import dag_object
        
        assert dag_object is not None
        assert dag_object.dag_id == "my_dag_task_flow"
        assert "simple_example_dag_test" in dag_object.tags

    def test_dag_has_correct_params(self):
        from dags.simple_example_dag_test.my_dag_task_flow import dag_object
        
        assert "my_param" in dag_object.params
        assert dag_object.params["my_param"] == 23

    @patch("airflow.sdk.Variable.get")
    @patch("airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook")
    @patch("airflow.sdk.Connection")
    def test_print_info_imports(self, mock_connection, mock_aws_hook, mock_variable_get):
        from dags.simple_example_dag_test.my_dag_task_flow import dag_object
        
        mock_variable_get.return_value = "test_value"
        mock_hook_instance = MagicMock()
        mock_aws_hook.return_value = mock_hook_instance
        
        context = {
            "logical_date": datetime(2025, 12, 15),
            "params": {"my_param": 23}
        }
        
        print_info_task = dag_object.task_dict["print_info"]
        print_info_task.python_callable(**context)

    @patch("airflow.sdk.Variable.get")
    @patch("airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook")
    def test_print_info_task(self, mock_aws_hook, mock_variable_get, capsys):
        from dags.simple_example_dag_test.my_dag_task_flow import dag_object
        
        mock_variable_get.return_value = "test_value"
        mock_hook_instance = MagicMock()
        mock_aws_hook.return_value = mock_hook_instance
        
        context = {
            "logical_date": datetime(2025, 12, 15),
            "params": {"my_param": 42}
        }
        
        print_info_task = dag_object.task_dict["print_info"]
        result = print_info_task.python_callable(**context)
        
        mock_variable_get.assert_called_once_with("my_variable")
        mock_aws_hook.assert_called_once_with(aws_conn_id="my_aws_conn")
        
        captured = capsys.readouterr()
        assert "Logical date:" in captured.out
        assert "my_param: 42" in captured.out
        assert "my_variable: test_value" in captured.out

    def test_dag_has_one_task(self):
        from dags.simple_example_dag_test.my_dag_task_flow import dag_object
        
        assert len(dag_object.tasks) == 1
        assert "print_info" in dag_object.task_dict


class TestMyDagTraditional:

    def test_dag_loads(self):
        from dags.simple_example_dag_test.my_dag_traditional import dag_object
        
        assert dag_object is not None
        assert dag_object.dag_id == "my_dag_traditional"
        assert "simple_example_dag_test" in dag_object.tags

    def test_print_hello_task(self, capsys):
        from dags.simple_example_dag_test.my_dag_traditional import dag_object
        
        print_hello_task = dag_object.task_dict["print_hello"]
        result = print_hello_task.python_callable()
        
        captured = capsys.readouterr()
        assert "Hello, World!" in captured.out

    def test_dag_has_one_task(self):
        from dags.simple_example_dag_test.my_dag_traditional import dag_object
        
        assert len(dag_object.tasks) == 1
        assert "print_hello" in dag_object.task_dict

    def test_dag_task_dependencies(self):
        from dags.simple_example_dag_test.my_dag_traditional import dag_object
        
        print_hello_task = dag_object.task_dict["print_hello"]
        assert len(print_hello_task.upstream_task_ids) == 0
        assert len(print_hello_task.downstream_task_ids) == 0

