import logging
import os
import random

import pytest

from pytest import fixture, mark

from dbnd import config
from dbnd._core.constants import CloudType, SparkClusters
from dbnd._core.errors import DatabandRunError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_airflow_contrib.mng_connections import add_connection, delete_connection
from dbnd_aws.env import AwsEnvConfig
from dbnd_databricks.databricks_config import DatabricksAwsConfig, DatabricksConfig
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
    _mvn_target_file,
)
from targets import target


@fixture(scope="session", autouse=True)
def setup_databricks_conn_id():
    conn_id = config.get("databricks", "conn_id")
    assert conn_id
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    conn_extra = '{"token": "' + str(token) + '","host": "' + str(host) + '"}'
    delete_connection(conn_id)
    add_connection(conn_id=conn_id, conn_type="databricks", host=host, extra=conn_extra)


conf_override_new_cluster = {
    "task": {"task_env": CloudType.aws},
    AwsEnvConfig.spark_engine: SparkClusters.databricks,
    AwsEnvConfig.root: "s3://dbnd-dev-databricks/int",
    DatabricksConfig.status_polling_interval_seconds: 60,
    DatabricksConfig.cloud_type: CloudType.aws,
    DatabricksAwsConfig.aws_instance_profile_arn: "arn:aws:iam::410311604149:instance-profile/dbnd_dev_databricks_ec2",
    DatabricksConfig.cluster_log_conf: {
        "s3": {
            "destination": "s3://dbnd-dev-databricks/int/logs",
            "region": "us-east-2",
        }
    },
    DatabricksConfig.spark_env_vars: {
        "DBND_HOME": "/home/ubuntu",
        "SQLALCHEMY_DATABASE_URI": "/home/ubuntu/.dbnd/dbnd.db",
        "WHL_S3_LOCATION": os.environ["DATABRICKS_WHL_LOCATION"],
    },
    DatabricksConfig.init_scripts: [
        {
            "s3": {
                "destination": os.environ["DATABRICKS_BOOTSTRAP_LOCATION"],
                "region": "us-east-2",
            }
        }
    ],
    SparkConfig.jars: [],
    SparkConfig.main_jar: os.environ["MAIN_SPARK_JAR"],
}

conf_override_existing_cluster = conf_override_new_cluster.copy()
conf_override_existing_cluster[DatabricksConfig.cluster_id] = os.environ[
    "DATABRICKS_CLUSTER_ID"
]

TEXT_FILE = "s3://dbnd-dev-databricks/int/data/README.md"


class TestDatabricksIntegrationNewCluster(object):
    @mark.databricks_int
    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text=TEXT_FILE,
            task_version=str(random.random()),
            override=conf_override_new_cluster,
        )
        actual.dbnd_run()

    @mark.databricks_int
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(
            text=TEXT_FILE,
            task_version=str(random.random()),
            override=conf_override_new_cluster,
        )
        actual.dbnd_run()

    @mark.databricks_int
    def test_word_spark_with_error(self):
        actual = WordCountThatFails(
            text=TEXT_FILE,
            task_version=str(random.random()),
            override=conf_override_new_cluster,
        )
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    @mark.databricks_int
    def test_word_count_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(
            word_count_inline.t(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override_new_cluster,
            )
        )


class TestDatabricksIntegrationExistingCluster(object):
    # probably we need to find out how to shutdown the cluster + change init script + start it.
    # however, this tests may fail on multiple runs because of race conditions
    @mark.databricks_int_existing
    @mark.skip
    def test_word_count_pyspark_existing_cluster(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text=TEXT_FILE,
            task_version=str(random.random()),
            override=conf_override_existing_cluster,
        )
        actual.dbnd_run()
