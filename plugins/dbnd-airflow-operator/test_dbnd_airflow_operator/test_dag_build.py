import copy
from typing import Tuple

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dbnd import task, pipeline
import pytest
from dbnd._core.errors import MissingParameterError

from test_dbnd_airflow_operator.airflow_home.dags.dag_test_examples import (
    default_args_test,
)


class TestFunctionalDagBuild(object):
    def test_task_is_bind_to_dag(self):
        @task
        def sample_callable():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bool_value = sample_callable()

        assert len(dag.tasks) == 1
        assert isinstance(bool_value, str)
        # TODO: assert type of dag.tasks[0]

    def test_multiple_outputs(self):
        @task
        def two_outputs() -> Tuple[str, str]:
            return "Nights", "Ni!"

        with DAG(dag_id="test_simple_build", default_args=default_args_test):
            a, b = two_outputs()

        assert isinstance(a, str)
        assert isinstance(b, str)

    def test_multiple_outputs_fail_without_type(self):
        @task
        def two_outputs():
            return "Nights", "Ni!"

        with pytest.raises(ValueError):
            with DAG(dag_id="test_simple_build", default_args=default_args_test):
                a, b = two_outputs()

    def test_tasks_are_bind_and_upstream_is_set(self):
        @task
        def task1(text):
            print(text)
            return True

        @task
        def task2(_):
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            a = task1("check")
            task2(a)

        a, b = dag.tasks
        assert a in b.upstream_list

    def test_tasks_error_when_no_param_is_passed(self):
        @task
        def task1(text):
            print(text)
            return True

        @task
        def task2(value):
            return True

        with pytest.raises(MissingParameterError):
            with DAG(dag_id="test_simple_build", default_args=default_args_test):
                task1("check")
                task2()

    @pytest.mark.xfail
    def test_task_and_chain_operator(self):
        @task
        def task1():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Ni!'")
            bash_task >> task1()

        _, b = dag.tasks
        assert bash_task in b.upstream_list

    def test_dag_config_build(self):
        @task
        def task1(p_str="value"):
            return True

        @task
        def task2(_):
            return False

        @pipeline
        def pipe(p_str="from_pipeline_ctor"):
            a = task1(p_str)
            return task2(a)

        args_test = copy.copy(default_args_test)
        args_test["dbnd_config"] = {pipe.task.p_str: "from_config"}

        with DAG(dag_id="test_dag_config", default_args=args_test) as dag_operators:
            pipe()

        assert dag_operators.task_dict["task1"].p_str == "from_config"
