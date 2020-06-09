import copy

from airflow import DAG

import dbnd

from test_dbnd_airflow.airflow_home.dags.dag_test_examples import (
    default_args_test,
    t_A,
    t_B,
    t_pipeline,
)
from test_dbnd_airflow.functional.utils import read_xcom_result_value, run_and_get


str(dbnd)

# we need all these dags at file level, so we can use standard airflow executor to run them.
with DAG(
    dag_id="test_simple_build", default_args=default_args_test
) as dag_simple_build:
    t_A("check", 2)

with DAG(
    dag_id="test_simple_run_wiring", default_args=default_args_test
) as dag_simple_run_wiring:
    a = t_A("check")
    b = t_B(a, expected="checkcheck")

with DAG(
    dag_id="dag_with_pipeline", default_args=default_args_test
) as dag_with_pipeline:
    t_pipeline()

default_args_config_dag = copy.copy(default_args_test)
default_args_config_dag["dbnd_config"] = {t_pipeline.task.p_str: "from_config"}

with DAG(
    dag_id="dag_with_config", default_args=default_args_config_dag
) as dag_with_config:
    t_pipeline()


class TestFunctionalDagRun(object):
    def test_simple_run(self):
        actual = run_and_get(dag_simple_build, "t_A")
        assert read_xcom_result_value(actual) == "checkcheck"

    def test_simple_2_run(self):
        assert len(dag_simple_run_wiring.tasks) == 2

        result = run_and_get(dag_simple_run_wiring, task_id="t_B")

        assert read_xcom_result_value(result) == "checkcheckt_B"

    def test_dag_pipeline_run(self):
        result = run_and_get(dag_with_pipeline, task_id="t_A")
        actual = read_xcom_result_value(result)
        assert actual == "from_pipeline_ctorfrom_pipeline_ctor"

    def test_dag_config_run(self):

        result = run_and_get(dag_with_config, task_id="t_A")
        actual = read_xcom_result_value(result)
        assert actual == "from_configfrom_config"
