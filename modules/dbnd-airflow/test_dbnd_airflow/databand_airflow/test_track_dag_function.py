from datetime import timedelta

import pytest

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from mock import Mock

from dbnd._core.decorator.dbnd_func_proxy import DbndFuncProxy
from dbnd_airflow.tracking.airflow_patching import patch_airflow_context_vars
from dbnd_airflow.tracking.dbnd_dag_tracking import track_dag
from dbnd_airflow.tracking.execute_tracking import track_operator


dbnd_spark_env_vars = (
    "spark.env.AIRFLOW_CTX_DAG_ID",
    "spark.env.AIRFLOW_CTX_EXECUTION_DATE",
    "spark.env.AIRFLOW_CTX_TASK_ID",
    "spark.env.AIRFLOW_CTX_TRY_NUMBER",
)


class TestTrackDagFunction(object):
    @pytest.fixture
    def dag(self):
        args = dict(start_date=days_ago(2))
        dag_object = DAG(
            dag_id="test_dag", default_args=args, schedule_interval=timedelta(minutes=1)
        )
        return dag_object

    @pytest.fixture
    def dataproc_operator(self, dag):
        from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

        operator = DataProcPySparkOperator(
            job_name="{{dag.dag_id}}__{{task.task_id}}_{{ts_nodash}}",
            cluster_name="{{var.dataproc_cluster_name}}",
            task_id="dataproc_task",
            main="script.py",
            arguments=["input.csv", "output.csv"],
            dag=dag,
        )
        track_dag(dag)
        return operator

    @pytest.mark.skip(
        reason="can't import google.cloud.storage in ci (used by DataProcPySparkOperator)"
    )
    def test_data_proc_pyspark_operator(self, dataproc_operator):
        for env_var in dbnd_spark_env_vars:
            assert env_var in dataproc_operator.dataproc_properties


class TestTrackOperator(object):
    @pytest.fixture
    def dag(self):
        args = dict(start_date=days_ago(2))
        dag_object = DAG(
            dag_id="test_dag", default_args=args, schedule_interval=timedelta(minutes=1)
        )
        return dag_object

    @pytest.fixture
    def python_operator(self, dag):
        def task1():
            return "output"

        operator = PythonOperator(task_id="python_task", python_callable=task1, dag=dag)
        track_operator({}, operator)
        return operator

    def test_python_operator(self, python_operator):
        assert isinstance(python_operator.python_callable, DbndFuncProxy)

    @pytest.fixture
    def emr_operator(self, dag):
        spark_submit_command = [
            "spark-submit",
            "--master",
            "yarn",
            "--name",
            "{{task.task_id}}",
            "script.py",
            "input.csv",
            "output.csv",
        ]

        step_command = dict(
            Name="{{task.task_id}}",
            ActionOnFailure="CONTINUE",
            HadoopJarStep=dict(Jar="command-runner.jar", Args=spark_submit_command),
        )

        operator = EmrAddStepsOperator(
            task_id="emr_task", job_flow_id=1, steps=[step_command], dag=dag
        )
        env = {
            "AIRFLOW_CTX_DAG_ID": "test_dag",
            "AIRFLOW_CTX_EXECUTION_DATE": "emr_task",
            "AIRFLOW_CTX_TASK_ID": "1970-01-01T0000.000",
            "AIRFLOW_CTX_TRY_NUMBER": "1",
        }

        track_operator(env, operator)
        return operator

    def test_emr_add_steps_operator(self, emr_operator):
        emr_args = emr_operator.steps[0]["HadoopJarStep"]["Args"]
        emr_args = {arg.split("=")[0] for arg in emr_args}

        for env_var in dbnd_spark_env_vars:
            assert env_var in emr_args

    @pytest.fixture
    def spark_submit_operator(self, dag):
        operator = SparkSubmitOperator(
            task_id="spark_submit_task",
            application="script.py",
            application_args=["input.csv", "output.csv"],
            dag=dag,
        )

        env = {
            "AIRFLOW_CTX_DAG_ID": "test_dag",
            "AIRFLOW_CTX_EXECUTION_DATE": "spark_submit_task",
            "AIRFLOW_CTX_TASK_ID": "1970-01-01T0000.000",
            "AIRFLOW_CTX_TRY_NUMBER": "1",
        }

        track_operator(env, operator)
        return operator

    def test_spark_submit_operator(self, spark_submit_operator):
        for env_var in dbnd_spark_env_vars:
            assert env_var in spark_submit_operator._conf

        assert "DBND__ENABLE__SPARK_CONTEXT_ENV" in spark_submit_operator._env_vars

    def test_airflow_context_vars_patch(self):
        patch_airflow_context_vars()
        from airflow.utils.operator_helpers import context_to_airflow_vars

        mock_task_instance = Mock(try_number=1)
        context = dict(task_instance=mock_task_instance)
        airflow_vars = context_to_airflow_vars(context)
        assert airflow_vars["AIRFLOW_CTX_TRY_NUMBER"] == "1"
