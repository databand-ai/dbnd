# © Copyright Databand.ai, an IBM Company 2022
import uuid

from datetime import timedelta

import mock
import pytest

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from mock import Mock

from dbnd._core.configuration.environ_config import (
    DBND_ROOT_RUN_UID,
    ENV_DBND_SCRIPT_NAME,
)
from dbnd_airflow.constants import AIRFLOW_VERSION_BEFORE_2_2
from dbnd_airflow.tracking.airflow_patching import patch_airflow_context_vars
from dbnd_airflow.tracking.config import TrackingSparkConfig
from dbnd_airflow.tracking.dbnd_dag_tracking import track_dag
from dbnd_airflow.tracking.wrap_operators import (
    get_airflow_operator_handlers_config,
    wrap_operator_with_tracking_info,
)
from dbnd_airflow.utils import get_airflow_instance_uid


if AIRFLOW_VERSION_BEFORE_2_2:
    from airflow.contrib.operators.ecs_operator import ECSOperator
    from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
else:
    from airflow.providers.amazon.aws.operators.ecs import ECSOperator
    from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator

dbnd_spark_env_vars = (
    "spark.env.AIRFLOW_CTX_DAG_ID",
    "spark.env.AIRFLOW_CTX_EXECUTION_DATE",
    "spark.env.AIRFLOW_CTX_TASK_ID",
    "spark.env.AIRFLOW_CTX_TRY_NUMBER",
    "spark.env.AIRFLOW_CTX_UID",
    "spark.env.DBND__TRACKING",
    "spark.yarn.appMasterEnv.DBND__TRACKING",
    "spark.yarn.appMasterEnv.SPARK_ENV_LOADED",
    "spark.yarn.appMasterEnv.DBND__ENABLE__SPARK_CONTEXT_ENV",
    "spark.yarn.appMasterEnv.DBND_HOME",
)


class TestTrackOperator(object):
    @pytest.fixture
    @mock.patch("dbnd_airflow.tracking.config.TrackingSparkConfig")
    def spark_config_with_agent(self, mock_config):
        stub_config = TrackingSparkConfig()
        stub_config.query_listener = True
        stub_config.agent_path = "/home/hadoop/dbnd-agent.jar"
        mock_config.from_databand_context.return_value = stub_config

        return mock_config

    @pytest.fixture
    @mock.patch("dbnd_airflow.tracking.config.TrackingSparkConfig")
    def spark_config_with_jar(self, mock_config):
        stub_config = TrackingSparkConfig()
        stub_config.query_listener = True
        stub_config.jar_path = "/home/hadoop/dbnd-agent.jar"
        mock_config.from_databand_context.return_value = stub_config

        return mock_config

    @pytest.fixture
    def dag(self):
        args = dict(start_date=days_ago(2))
        dag_object = DAG(
            dag_id="test_dag", default_args=args, schedule_interval=timedelta(minutes=1)
        )
        return dag_object

    @pytest.fixture
    def tracking_info(self):
        return {
            "AIRFLOW_CTX_DAG_ID": "test_dag",
            "AIRFLOW_CTX_EXECUTION_DATE": "emr_task",
            "AIRFLOW_CTX_TASK_ID": "1970-01-01T0000.000",
            "AIRFLOW_CTX_TRY_NUMBER": "1",
            "AIRFLOW_CTX_UID": get_airflow_instance_uid(),
            DBND_ROOT_RUN_UID: str(uuid.uuid4()),
        }

    def test_emr_add_steps_operator(self, tracking_info, dag, spark_config_with_agent):
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

        emr_operator = EmrAddStepsOperator(
            task_id="emr_task", job_flow_id=1, steps=[step_command], dag=dag
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()

        with wrap_operator_with_tracking_info(
            tracking_info, emr_operator, airflow_operator_handlers
        ):
            pass

        emr_args = emr_operator.steps[0]["HadoopJarStep"]["Args"]
        emr_args_names = {arg.split("=")[0] for arg in emr_args}

        for env_var in dbnd_spark_env_vars:
            assert env_var in emr_args_names
        spark_submit_string = " ".join(emr_args)
        assert (
            "--conf spark.driver.extraJavaOptions=-javaagent:/home/hadoop/dbnd-agent.jar"
            in spark_submit_string
        )
        assert (
            "--conf spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener"
            in spark_submit_string
        )

    def test_pyspark_submit_operator(self, tracking_info, dag, spark_config_with_agent):
        pyspark_submit_operator = SparkSubmitOperator(
            task_id="spark_submit_task",
            application="script.py",
            application_args=["input.csv", "output.csv"],
            dag=dag,
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()
        with wrap_operator_with_tracking_info(
            tracking_info, pyspark_submit_operator, airflow_operator_handlers
        ):
            pass
        conf = pyspark_submit_operator._conf
        for env_var in dbnd_spark_env_vars:
            assert env_var in conf

        assert "DBND__ENABLE__SPARK_CONTEXT_ENV" in pyspark_submit_operator._env_vars
        assert (
            conf["spark.driver.extraJavaOptions"]
            == "-javaagent:/home/hadoop/dbnd-agent.jar"
        )
        assert (
            conf["spark.sql.queryExecutionListeners"]
            == "ai.databand.spark.DbndSparkQueryExecutionListener"
        )

    def test_spark_submit_operator(self, dag, spark_config_with_jar):
        spark_submit_operator = SparkSubmitOperator(
            task_id="spark_submit_task",
            conf={"spark.jars": "dbnd-examples-all.jar"},
            application_args=["input.csv", "output.csv"],
            dag=dag,
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()
        with wrap_operator_with_tracking_info(
            {}, spark_submit_operator, airflow_operator_handlers
        ):
            pass
        conf = spark_submit_operator._conf

        assert conf["spark.jars"] == "dbnd-examples-all.jar,/home/hadoop/dbnd-agent.jar"
        assert (
            conf["spark.sql.queryExecutionListeners"]
            == "ai.databand.spark.DbndSparkQueryExecutionListener"
        )

    def test_ecs_operator(self, tracking_info, dag):
        ecs_operator = ECSOperator(
            task_id="ecs_task",
            dag=dag,
            cluster="cluster",
            task_definition="definition",
            overrides={
                "containerOverrides": [{"command": ["some", "command"], "cpu": 50}]
            },
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()
        with wrap_operator_with_tracking_info(
            tracking_info, ecs_operator, airflow_operator_handlers
        ):
            pass
        for override in ecs_operator.overrides["containerOverrides"]:
            assert {"name": "AIRFLOW_CTX_TRY_NUMBER", "value": "1"} in override[
                "environment"
            ]

    def test_ecs_operator_with_env(self, tracking_info, dag):
        ecs_operator_with_env = ECSOperator(
            task_id="ecs_task",
            dag=dag,
            cluster="cluster",
            task_definition="definition",
            overrides={
                "containerOverrides": [
                    {
                        "command": ["some", "command"],
                        "cpu": 50,
                        "environment": [{"name": "USER_KEY", "value": "USER_VALUE"}],
                    }
                ]
            },
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()
        with wrap_operator_with_tracking_info(
            tracking_info, ecs_operator_with_env, airflow_operator_handlers
        ):
            for override in ecs_operator_with_env.overrides["containerOverrides"]:
                assert {"name": "AIRFLOW_CTX_TRY_NUMBER", "value": "1"} in override[
                    "environment"
                ]
                assert {"name": "USER_KEY", "value": "USER_VALUE"} in override[
                    "environment"
                ]

    def test_databricks_operator_with_env(
        self, tracking_info, dag, spark_config_with_agent
    ):
        databricks_cluster_params = {
            "spark_version": "6.5.x-scala2.11",
            "node_type_id": "m5a.large",
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "ebs_volume_count": 1,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_size": 100,
            },
            "spark_env_vars": {"DBND__VERBOSE": "True"},
            "num_workers": 1,
        }

        databricks_task_params = {
            "name": "generate rport",
            "new_cluster": databricks_cluster_params,
            "libraries": [{"pypi": {"package": "dbnd"}}],
            "max_retries": 1,
            "spark_python_task": {
                "python_file": "s3://databricks/scripts/databricks_report.py"
            },
        }

        databricks_operator_with_env = DatabricksSubmitRunOperator(
            task_id="databricks_task", json=databricks_task_params
        )

        airflow_operator_handlers = get_airflow_operator_handlers_config()
        with wrap_operator_with_tracking_info(
            tracking_info, databricks_operator_with_env, airflow_operator_handlers
        ):
            config = databricks_operator_with_env.json
            env = config["new_cluster"]["spark_env_vars"]
            assert env["AIRFLOW_CTX_TRY_NUMBER"] == "1"
            assert env[ENV_DBND_SCRIPT_NAME] == "databricks_report.py"
            spark_conf = config["new_cluster"]["spark_conf"]
            assert (
                spark_conf["spark.driver.extraJavaOptions"]
                == "-javaagent:/home/hadoop/dbnd-agent.jar"
            )
            assert (
                spark_conf["spark.sql.queryExecutionListeners"]
                == "ai.databand.spark.DbndSparkQueryExecutionListener"
            )

    def test_airflow_context_vars_patch(self):
        patch_airflow_context_vars()
        from airflow.utils.operator_helpers import context_to_airflow_vars

        mock_task_instance = Mock(try_number=1)
        context = dict(task_instance=mock_task_instance)
        airflow_vars = context_to_airflow_vars(context)
        assert airflow_vars["AIRFLOW_CTX_TRY_NUMBER"] == "1"

    @pytest.mark.skip(
        reason="can't import google.cloud.storage in ci (used by DataProcPySparkOperator)"
    )
    def test_dataproc_pyspark_operator(self, dag):
        from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

        dataproc_operator = DataProcPySparkOperator(
            job_name="{{dag.dag_id}}__{{task.task_id}}_{{ts_nodash}}",
            cluster_name="{{var.dataproc_cluster_name}}",
            task_id="dataproc_task",
            main="script.py",
            arguments=["input.csv", "output.csv"],
            dag=dag,
        )
        track_dag(dag)

        for env_var in dbnd_spark_env_vars:
            assert env_var in dataproc_operator.dataproc_properties
