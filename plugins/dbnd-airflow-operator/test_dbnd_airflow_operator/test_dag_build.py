import copy

from typing import Tuple

import pytest

from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from dbnd import pipeline, task
from dbnd._core.errors import MissingParameterError
from test_dbnd_airflow_operator.airflow_home.dags.dag_test_examples import (
    default_args_test,
)


class TestFunctionalOperatorAirflowKwargs:
    def test_airflow_op_kwargs_are_set(self):
        @task(task_airflow_op_kwargs={"trigger_rule": TriggerRule.ALL_FAILED})
        def sample_callable():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bool_value = sample_callable()
            assert bool_value.op.trigger_rule == TriggerRule.ALL_FAILED

    def test_airflow_op_kwargs_error_if_not_kwarg(self):
        @task(task_airflow_op_kwargs={"nonExistingField": "vaaalue"})
        def sample_callable():
            return True

        # This should rise an error during parsing
        with pytest.raises(AttributeError):
            with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
                bool_value = sample_callable()


class TestFunctionalDagBuild(object):
    @staticmethod
    def is_xcom_str(x):
        # type: (str) -> bool
        return "task_instance.xcom_pull" in x

    def test_task_is_bind_to_dag(self):
        @task
        def sample_callable():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bool_value = sample_callable()

        assert len(dag.tasks) == 1
        assert isinstance(bool_value, str)
        assert self.is_xcom_str(bool_value)

    def test_multiple_outputs_with_type_annotation(self):
        @task
        def two_outputs():
            # type:  () -> Tuple[str, str]
            return "Nights", "Ni!"

        with DAG(dag_id="test_simple_build", default_args=default_args_test):
            a, b = two_outputs()

        assert isinstance(a, str)
        assert isinstance(b, str)
        assert self.is_xcom_str(a)
        assert self.is_xcom_str(b)

    def test_multiple_outputs_fail_without_type(self):
        @task
        def two_outputs():
            return "Nights", "Ni!"

        with pytest.raises(ValueError) as err:
            with DAG(dag_id="test_simple_build", default_args=default_args_test):
                _, _ = two_outputs()
            err.match(".*type annotations.*")

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

        a, b = sorted(dag.tasks, key=lambda op: op.task_id)  # Py2.7 order
        assert b in a.downstream_list

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

    def test_task_and_operator_lshift_fail(self):
        @task
        def task1():
            return True

        with pytest.raises(AirflowException):
            with DAG(dag_id="test_simple_build", default_args=default_args_test):
                bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Ni!'")
                bash_task >> task1()  # should be task1().op -> next test

    def test_task_and_operator_lshift_requires_op(self):
        @task
        def task1():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Ni!'")
            bash_task >> task1().op

        _, t = sorted(dag.tasks, key=lambda op: op.task_id)  # Py2.7 order
        assert bash_task in t.upstream_list

    def test_task_set_upstream_using_lshift(self):
        @task
        def task1():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Ni!'")
            task1() >> bash_task

        b, t = sorted(dag.tasks, key=lambda op: op.task_id)  # Py2.7 order
        assert t in b.upstream_list

    def test_task_set_downstream_using_rshift(self):
        @task
        def task1():
            return True

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Ni!'")
            task1() << bash_task

        b, t = sorted(dag.tasks, key=lambda op: op.task_id)  # Py2.7 order
        assert t in b.downstream_list

    def test_multiple_value_output_chain_operations(self):
        @task
        def two_outputs():
            # type:  () -> Tuple[str, str]
            return "Nights", "Ni!"

        with DAG(dag_id="test_simple_build", default_args=default_args_test) as dag:
            bash_task1 = BashOperator(task_id="bash_task1", bash_command="echo 'Ni!'")
            bash_task2 = BashOperator(task_id="bash_task2", bash_command="echo 'Ni!'")
            t = two_outputs()
            t >> bash_task1
            t << bash_task2

            assert t.op in bash_task1.upstream_list
            assert t.op in bash_task2.downstream_list

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
