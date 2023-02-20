# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List

from dbnd import parameter
from dbnd._core.constants import SparkClusters
from dbnd._core.task.config import Config
from dbnd_spark.spark_config import SparkEngineConfig


logger = logging.getLogger(__name__)


class DatabricksCloud(object):
    aws = "aws"
    azure = "azure"


class DatabricksConfig(SparkEngineConfig):
    """Databricks cloud for Apache Spark"""

    _conf__task_family = "databricks"
    cluster_type = SparkClusters.databricks

    stop_session_on_finish = False

    cluster_id = parameter(default=None).help("Set the ID of the existing cluster.")[
        str
    ]
    cloud_type = parameter().help(
        "Set the type of the cloud. This can either be AWS or Azure."
    )[str]

    conn_id = parameter.value(default="databricks_default").help(
        "Set Databricks' connection ID."
    )[str]

    connection_retry_limit = parameter.value(default=3).help(
        "Set the retry limit of the databricks connection."
    )[int]

    connection_retry_delay = parameter.value(default=1).help(
        "Set the Databricks connection's delay in between retries."
    )[int]

    status_polling_interval_seconds = parameter(default=10).help(
        "Set the number of seconds to sleep between polling databricks for a job's status."
    )[int]

    cluster_log_conf = parameter(default={}).help(
        "Set the location that will be used for logs, e.g.: "
        '`{"s3": {"destination": "s3://<BUCKET>/<KEY>", "region": "us-east-1"}}`'
    )

    # new cluster config
    num_workers = parameter(default=0).help(
        "Set the number of workers as in databricks' api."
    )[int]
    init_scripts = parameter(default=[]).help(
        "Set the init script list. The default for this is "
        "`{ 's3': { 'destination' : 's3://init_script_bucket/prefix', 'region' : 'us-west-2' } }`"
    )[List]
    spark_version = parameter().help("Set the Spark version that will be used.")[str]
    spark_conf = parameter(default={}).help(
        "Determine Spark's configuration settings."
    )[Dict]
    node_type_id = parameter(default="").help("Nodes for spark machines")[str]
    spark_env_vars = parameter(default={}).help("Set Spark's environment variables.")[
        Dict
    ]

    def get_spark_ctrl(self, task_run):
        from dbnd_databricks.databricks import DatabricksCtrl

        return DatabricksCtrl(task_run=task_run)

    def _validate(self):
        super(DatabricksConfig, self)._validate()
        if not self.cluster_id:
            logger.warning(
                "no databricks.cluster_id is set, will create a new databricks cluster - please remember"
                " to configure your cluster parameters."
            )


class DatabricksAzureConfig(Config):
    _conf__task_family = "databricks_azure"
    local_dbfs_mount = parameter(description="local mount for dbfs")[str]


class DatabricksAwsConfig(Config):
    _conf__task_family = "databricks_aws"

    # aws machine related.
    ebs_count = parameter(default=1).help("Number of nodes for spark machines.")
    aws_instance_profile_arn = parameter(default="<>").help(
        "Set the IAM profile for spark machines."
    )[str]

    aws_ebs_volume_type = parameter(default="GENERAL_PURPOSE_SSD").help(
        "Set the EBS type."
    )[str]
    aws_ebs_volume_count = parameter(default=1).help("Set the number of EBS volumes.")[
        int
    ]
    aws_ebs_volume_size = parameter(default=100).help("Set the size of EBS volume.")[
        int
    ]
