from datetime import timedelta

import pytest

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dbnd._core.decorator.func_task_decorator import _decorated_user_func
from dbnd._core.inplace_run.airflow_utils import track_dag


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
    def python_operator(self, dag):
        def task1():
            return "output"

        operator = PythonOperator(task_id="python_task", python_callable=task1, dag=dag)
        track_dag(dag)
        return operator

    def test_python_operator(self, python_operator):
        assert isinstance(python_operator.python_callable, _decorated_user_func)

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
        track_dag(dag)
        return operator

    def test_emr_add_steps_operator(self, emr_operator):
        emr_args = emr_operator.steps[0]["HadoopJarStep"]["Args"]
        emr_args = {arg.split("=")[0] for arg in emr_args}

        for env_var in dbnd_spark_env_vars:
            assert env_var in emr_args

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

    @pytest.fixture
    def spark_submit_operator(self, dag):
        operator = SparkSubmitOperator(
            task_id="spark_submit_task",
            application="script.py",
            application_args=["input.csv", "output.csv"],
            dag=dag,
        )
        track_dag(dag)
        return operator

    def test_spark_submit_operator(self, spark_submit_operator):
        for env_var in dbnd_spark_env_vars:
            assert env_var in spark_submit_operator._conf

        assert "DBND__ENABLE__SPARK_CONTEXT_ENV" in spark_submit_operator._env_vars
