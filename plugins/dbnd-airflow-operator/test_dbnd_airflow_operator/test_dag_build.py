import copy

from airflow import DAG

from test_dbnd_airflow_operator.airflow_home.dags.dag_test_examples import (
    default_args_test,
    t_A,
    t_B,
    t_pipeline,
    t_two_outputs,
)


class TestFunctionalDagBuild(object):
    def test_simple_build(self):
        with DAG(
            dag_id="test_simple_build", default_args=default_args_test
        ) as dag_operators:
            a = t_A("check", 2)
        assert len(dag_operators.tasks) == 1
        assert a
        assert isinstance(a, str)

    def test_simple_wiring(self):
        with DAG(
            dag_id="test_simple_build", default_args=default_args_test
        ) as dag_operators:
            a = t_A("check")
            b = t_B(a)

        assert a
        assert b

        assert len(dag_operators.tasks) == 2

    def test_multiple_outputs(self):
        with DAG(
            dag_id="test_simple_build", default_args=default_args_test
        ) as dag_operators:
            result = t_two_outputs("check")

        assert len(dag_operators.tasks) == 1
        assert result
        a, b = result
        assert isinstance(a, str)
        assert isinstance(b, str)

    def test_dag_config_build(self):
        args_test = copy.copy(default_args_test)
        args_test["dbnd_config"] = {t_pipeline.task.p_str: "from_config"}
        with DAG(dag_id="test_dag_config", default_args=args_test) as dag_operators:
            t_pipeline()

        assert dag_operators.task_dict["t_A"].p_str == "from_config"
