from databand.parameters import (
    DateTimeParameter,
    DictParameter,
    IntParameter,
    ListParameter,
    Parameter,
)
from dbnd import parameter
from dbnd._core.constants import ClusterPolicy, SparkClusters
from dbnd._core.errors import DatabandConfigError
from dbnd_spark.spark_config import SparkEngineConfig


class DataprocConfig(SparkEngineConfig):
    """Google Cloud Dataproc"""

    _conf__task_family = "dataproc"

    cluster_type = SparkClusters.dataproc

    cluster = Parameter.c(description="Cluster name")

    policy = parameter.c(
        default=ClusterPolicy.NONE, description="Cluster start/stop policy"
    ).choices(choices=ClusterPolicy.ALL)

    region = Parameter.c(default="global", description="gcp region")

    zone = Parameter(
        description="The zone where the cluster will be located. (templated)"
    )
    num_workers = IntParameter(description="The # of workers to spin up")

    num_preemptible_workers = IntParameter(
        default=0, description="The # of preemptible worker nodes to spin up"
    )
    network_uri = Parameter.c(
        description="The network uri to be used for machine communication, "
        "cannot be specified with subnetwork_uri"
    )
    subnetwork_uri = Parameter.c(
        description="""The subnetwork uri to be used for machine communication,
        cannot be specified with network_uri"""
    )

    tags = ListParameter.c(description="The GCE tags to add to all instances")

    storage_bucket = Parameter.c(
        description="The storage bucket to use, "
        "setting to None lets dataproc generate a custom one for you"
    )
    init_actions_uris = ListParameter.c(
        description="List of GCS uri's containing dataproc initialization scripts" ""
    )
    init_action_timeout = Parameter(
        default="10m",
        description=""" Amount of time executable scripts in
        init_actions_uris has to complete""",
    )[str]

    metadata = DictParameter.c(
        description="""dict of key-value google compute engine metadata entries
        to add to all instances"""
    )

    image_version = Parameter.c(
        description="the version of software inside the Dataproc cluster"
    )
    properties = DictParameter.c(
        description="""dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ \
        projects.regions.clusters#SoftwareConfig"""
    )

    master_machine_type = Parameter(
        description="Compute engine machine type to use for the master node"
    )
    master_disk_size = IntParameter(
        default=500, description="Disk size for the master node"
    )

    worker_machine_type = Parameter(
        description="Compute engine machine type to use for the worker nodes"
    )
    worker_disk_size = IntParameter(
        default=500, description="Disk size for the worker nodes"
    )

    labels = ListParameter.c(description="")
    delegate_to = Parameter.c(
        description="""The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled."""
    )
    service_account = Parameter.c(
        description=" The service account of the dataproc instances."
    )
    service_account_scopes = ListParameter.c(
        description="The URIs of service account scopes to be included."
    )

    idle_delete_ttl = IntParameter.c(
        description="""The longest duration that cluster would keep alive while
        staying idle. Passing this threshold will cause cluster to be auto-deleted.
        A duration in seconds."""
    )
    auto_delete_time = DateTimeParameter.c(
        description="The time when cluster will be auto-deleted."
    )
    auto_delete_ttl = IntParameter.c(
        description="""The life duration of cluster, the cluster will be
        auto-deleted at the end of this duration.
        A duration in seconds. (If auto_delete_time is set this parameter will be ignored)"""
    )

    def _validate(self):
        super(DataprocConfig, self)._validate()
        if not self.cluster:
            raise DatabandConfigError(
                "Spark cluster name is not defined! Use --dataproc-cluster CLUSTERNAME "
            )

    def get_spark_ctrl(self, task_run):
        from dbnd_gcp.dataproc.dataproc import DataProcCtrl

        return DataProcCtrl(task_run=task_run)
