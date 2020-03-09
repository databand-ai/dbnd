import copy
import logging

from datetime import timedelta
from typing import Tuple

from airflow import DAG
from airflow.models import XCom
from airflow.utils.dates import days_ago

from dbnd import dbnd_config, pipeline, task
from dbnd._core.utils.timezone import utcnow
from dbnd_airflow.executors.simple_executor import InProcessExecutor
from targets import target


default_args_test = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


@task
def t_A(p_str="check", p_int=2) -> str:
    logging.info("I am running")
    return p_str * p_int


@task
def t_B(pb_str="check", expected=None) -> str:
    if expected:
        assert pb_str == expected
    logging.info("I am running")
    return pb_str + "t_B"


@task
def t_two_outputs(pb_str="check") -> Tuple[str, int]:
    logging.info("t_two_outputs is running")
    return pb_str + "t_B", len(pb_str)


@task
def t_with_fields(p_str="from_ctor", p2_str="from_ctor") -> str:
    return p_str + p2_str


@pipeline
def t_pipeline(p_str="from_pipeline_ctor") -> str:
    dbnd_config.log_layers()
    a = t_A(p_str)
    return t_B(a)


class TestFunctionalDagBuild(object):
    def test_simple_build(self):
        with DAG(
            dag_id="test_simple_build", default_args=default_args_test
        ) as dag_operators:
            t_A("check", 2)

        assert len(dag_operators.tasks) == 1

    def test_simple_wiring(self):
        with DAG(
            dag_id="test_simple_build", default_args=default_args_test
        ) as dag_operators:
            a = t_A("check")
            b = t_B(a)

        assert len(dag_operators.tasks) == 2

    def test_simple_run_wiring(self):
        with DAG(
            dag_id="test_simple_run_wiring", default_args=default_args_test
        ) as dag_operators:
            a = t_A("check")
            b = t_B(a, expected="checkcheck")
        assert len(dag_operators.tasks) == 2

        result = run_and_get(dag_operators, task_id="t_B")

        b_result = result["result"]
        actual = target(b_result).read()
        assert actual == "checkcheckt_B"

    def test_dag_config(self):
        args_test = copy.copy(default_args_test)
        args_test["dbnd_config"] = {t_pipeline.task.p_str: "from_config"}
        with DAG(dag_id="test_dag_config", default_args=args_test) as dag_operators:
            t_pipeline()

        result = run_and_get(dag_operators, task_id="t_A")

        b_result = result["result"]
        actual = target(b_result).read()
        assert actual == "from_configfrom_config"

    # def test_aws_config(self):
    #
    #     args_test = copy.copy(default_args_test)
    #     args_test["dbnd_config"] = {"databand": {"env": "aws"}}
    #     with DAG(dag_id="test_aws_config", default_args=args_test) as dag_operators:
    #         t_A()
    #
    #     result = run_and_get(dag_operators, task_id="t_A")
    #     b_result = result["result"]
    #     assert b_result.startswith("s3://dbnd-dev-playground/databand_project/dev/")


def run_and_get(dag, task_id, execution_date=None):
    execution_date = utcnow()
    _run_dag(dag, execution_date=execution_date)
    return _get_result(dag, task_id, execution_date=execution_date)


def _run_dag(dag, execution_date=None):
    execution_date = execution_date or days_ago(0)
    dag.run(
        start_date=execution_date, end_date=execution_date, executor=InProcessExecutor()
    )


def _get_result(dag, task_id, execution_date=None):
    execution_date = execution_date or days_ago(0)
    result = XCom.get_one(
        execution_date=execution_date, task_id=task_id, dag_id=dag.dag_id
    )
    assert result
    return result
