import logging

from typing import Dict, List

from dbnd import parameter
from dbnd._core.constants import SparkClusters
from dbnd._core.task.config import Config


logger = logging.getLogger(__name__)


class DatabricksConfig(Config):
    """Databricks cloud for Apache Spark """

    _conf__task_family = "databricks"
    cluster_type = SparkClusters.databricks
    conn_id = parameter.value(default="databricks_default").help(
        "databricks connection settings"
    )[str]
    connection_retry_limit = parameter.value(default=3).help(
        "databricks connection - retry limit"
    )[int]
    connection_retry_delay = parameter.value(default=1).help(
        "databricks connection - delay in between retries"
    )[int]
    conn_id = parameter.value(default="databricks_default").help(
        "databricks connection settings"
    )[str]
    cluster_id = parameter(default="None").help("existing cluster id")[str]
    # new cluster config
    num_workers = parameter.help("number of workers as in databricks api.")[int]
    spark_version = parameter.help("spark version")[str]
    spark_conf = parameter(default={}).help("spark config")[Dict]
    node_type_id = parameter.help("nodes for spark machines")[str]
    aws_instance_profile_arn = parameter.help("IAM profile for spark machines")[str]
    spark_env_vars = parameter.c.help("spark env vars")[Dict]
    ebs_count = parameter(default=1).help("nodes for spark machines")
    # aws machine related.
    aws_ebs_volume_type = parameter(default="GENERAL_PURPOSE_SSD").help("EBS type")[str]
    aws_ebs_volume_count = parameter(default=1).help("num of EBS volumes")[int]
    aws_ebs_volume_size = parameter(default=100).help("size of EBS volume")[int]

    # SHOULDDO: replace this init script with native databricks library support.
    init_script = parameter.c.help("List of init scripts to run.")[List]
    status_polling_interval_seconds = parameter(default=10.0).help(
        "seconds to sleep between polling databricks for job status."
    )[int]
    cluster_log_conf = parameter.c.help("logs location")[Dict]

    def get_spark_ctrl(self, task_run):
        from dbnd_databricks.databricks import DatabricksCtrl

        return DatabricksCtrl(task_run=task_run)

    def _validate(self):
        super(DatabricksConfig, self)._validate()
        if self.cluster_id is "None":
            logger.warning(
                "no databricks.cluster_id is set, will create a new databricks cluster - please remember"
                " to configure your cluster parameters."
            )
