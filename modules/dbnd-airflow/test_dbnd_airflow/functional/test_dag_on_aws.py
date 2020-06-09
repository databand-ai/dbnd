import copy
import os

import pytest

from airflow import DAG

from dbnd._core.settings import CoreConfig, DatabandSystemConfig, EnvConfig
from test_dbnd_airflow.airflow_home.dags.dag_test_examples import default_args_test, t_A
from test_dbnd_airflow.functional.utils import run_and_get


TEST_ENV__AWS_ROOT = os.environ.get("TEST_ENV__AWS_ROOT")


class TestDagOnAws(object):
    @pytest.mark.importorskip("dbnd_aws")
    def test_aws_config(self):
        args_test = copy.copy(default_args_test)
        args_test["dbnd_config"] = {
            DatabandSystemConfig.env: "aws",
            CoreConfig.environments: ["aws"],
            "aws": {EnvConfig.root: "s3://ssss"},
        }
        with DAG(dag_id="test_aws_config", default_args=args_test) as dag_operators:
            t_A()

        t_a = dag_operators.task_dict["t_A"]

    @pytest.mark.skipif("not TEST_ENV__AWS_ROOT")
    def test_aws_config(self):
        args_test = copy.copy(default_args_test)
        args_test["dbnd_config"] = {
            "databand": {"env": "aws"},
            "aws": {EnvConfig.root: TEST_ENV__AWS_ROOT},
        }
        with DAG(dag_id="test_aws_config", default_args=args_test) as dag_operators:
            t_A()

        result = run_and_get(dag_operators, task_id="t_A")
        b_result = result["result"]
        assert b_result.startswith("s3://dbnd-test/databand_project/dev/")
