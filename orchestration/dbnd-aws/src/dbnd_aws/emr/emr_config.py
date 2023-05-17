# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from dbnd._core.constants import ClusterPolicy, EmrClient, SparkClusters
from dbnd._core.context.use_dbnd_run import assert_airflow_package_installed
from dbnd._core.errors import DatabandConfigError
from dbnd_spark.spark_config import SparkEngineConfig


class EmrConfig(SparkEngineConfig):
    """Amazon Elastic MapReduce"""

    _conf__task_family = "emr"

    cluster_type = SparkClusters.emr
    cluster = parameter.c(description="Set the cluster's name.")[str]

    policy = parameter.c(
        default=ClusterPolicy.NONE,
        description="Determine the cluster's start/stop policy.",
    ).choices(ClusterPolicy.ALL)

    region = parameter.c(
        description="Determine the region to use for the AWS connection.",
        default="us-east-2",
    )[str]

    conn_id = parameter.value(
        default="spark_emr", description="Set Spark emr connection settings."
    )

    action_on_failure = parameter.c(
        description="Set an action to take on failure of Spark submit. E.g. `CANCEL_AND_WAIT` or `CONTINUE`.",
        default="CANCEL_AND_WAIT",
    )[str]

    emr_completion_poll_interval = parameter(
        default=10,
        description="Set the numbber of seconds to wait between polls of step completion job.",
    )[int]

    client = parameter.c(
        default=EmrClient.STEP,
        description="Set the type of client used to run EMR jobs.",
    )
    ssh_mode = parameter.c(
        description="If this is enabled,  use localhost:SSH_PORT(8998) to connect to livy.",
        default=False,
    )[bool]
    livy_port = parameter[int].c(
        description="Set which port will be used to connect to livy.", default=8998
    )

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
