from dbnd import parameter
from dbnd._core.constants import ClusterPolicy, EmrClient, SparkClusters
from dbnd._core.errors import DatabandConfigError
from dbnd._core.plugin.dbnd_plugins import assert_airflow_package_installed
from dbnd_spark.spark_config import SparkEngineConfig


class EmrConfig(SparkEngineConfig):
    """Amazon Elastic MapReduce"""

    _conf__task_family = "emr"

    cluster_type = SparkClusters.emr
    cluster = parameter.c(description="Cluster name")[str]

    policy = parameter.c(
        default=ClusterPolicy.NONE, description="Cluster start/stop policy"
    ).choices(ClusterPolicy.ALL)

    region = parameter.c(
        description="region to use for aws connection", default="us-east-2"
    )[str]

    conn_id = parameter.value(
        default="spark_emr", description="spark emr connection settings"
    )

    action_on_failure = parameter.c(
        description="Action to take on failure of spark submit CANCEL_AND_WAIT/CONTINUE",
        default="CANCEL_AND_WAIT",
    )[str]

    emr_completion_poll_interval = parameter(
        default=10, description="Seconds to wait between polls of step completion job"
    )[int]

    client = parameter.c(
        default=EmrClient.STEP, description="Type of client used to run EMR jobs"
    )
    ssh_mode = parameter.c(
        description="If ssh mode is on - we'll use localhost:SSH_PORT(8998) to connect to livy",
        default=False,
    )[bool]
    livy_port = parameter[int].c(description="Port to connect to livy", default=8998)

    def _validate(self):
        super(EmrConfig, self)._validate()
        if not self.cluster:
            raise DatabandConfigError(
                "Spark cluster name is not defined! Use --set emr.cluster <CLUSTERNAME> "
            )

    def get_spark_ctrl(self, task_run):

        from dbnd._core.constants import EmrClient

        if self.client == EmrClient.LIVY:
            from dbnd_aws.emr.emr_ctrl import EmrLivyCtr

            return EmrLivyCtr(task_run=task_run)
        else:
            assert_airflow_package_installed()
            from dbnd_aws.emr.emr_step import EmrStepCtrl

            return EmrStepCtrl(task_run=task_run)
