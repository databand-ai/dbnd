import logging
import os
import random
import subprocess

from pytest import fixture, mark

from dbnd import config
from dbnd._core.constants import CloudType, SparkClusters
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_aws.env import AwsEnvConfig
from dbnd_databricks.databricks_config import DatabricksConfig
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
    _mvn_target_file,
)
from targets import target


@fixture(scope="session", autouse=True)
def init_airflow_db():
    sql_alch_conn = config.get("core", "sql_alchemy_conn")
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    conn_extra = '{"token": "' + str(token) + '","host": "' + str(host) + '"}'
    subprocess.check_call(
        [
            "dbnd",
            "airflow",
            "connections",
            "--delete",
            "--conn_id",
            "databricks_default",
        ],
        env={"AIRFLOW__CORE__SQL_ALCHEMY_CONN": sql_alch_conn},
    )
    subprocess.check_call(
        [
            "dbnd",
            "airflow",
            "connections",
            "--add",
            "--conn_id",
            "databricks_default",
            "--conn_type",
            "databricks",
            "--conn_host",
            host,
            "--conn_extra",
            conn_extra,
        ],
        env={"AIRFLOW__CORE__SQL_ALCHEMY_CONN": sql_alch_conn},
    )


conf_override_new_cluster = {
    "task": {"task_env": CloudType.aws},
    AwsEnvConfig.spark_engine: SparkClusters.databricks,
    AwsEnvConfig.root: "s3://dbnd-databricks/int_tests",
    DatabricksConfig.status_polling_interval_seconds: 10,
    DatabricksConfig.cloud_type: CloudType.aws,
    SparkConfig.jars: "",
    SparkConfig.main_jar: _mvn_target_file("ai.databand.examples-1.0-SNAPSHOT.jar"),
}

conf_override_existing_cluster = conf_override_new_cluster.copy()
conf_override_existing_cluster[DatabricksConfig.cluster_id] = (
    os.environ["DATABRICKS_CLUSTER_ID"],
)

TEXT_FILE = "s3://dbnd-databricks/README.md"


class TestDatabricksIntegration(object):
    @mark.databricks_int
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(
            text=TEXT_FILE,
            task_version=str(random.random()),
            override=conf_override_existing_cluster,
        )
        actual.dbnd_run()
        # print(target(actual.counters.path, "part-00000").read())
