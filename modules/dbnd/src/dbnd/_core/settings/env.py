import logging

from dbnd._core.constants import ApacheBeamClusterType, CloudType, EnvLabel
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.parameter_definition import ParameterScope
from dbnd._core.task.config import Config
from targets import DirTarget


logger = logging.getLogger(__name__)

task_env_param = parameter(scope=ParameterScope.children)


class EnvConfig(Config):
    """Databand's environment"""

    _conf__task_family = "env"
    cloud_type = parameter(description="cloud type: gcp/aws/")[str]

    env_label = parameter(
        default=EnvLabel.dev, description="environment type: dev/int/prod"
    )[
        str
    ]  # label

    production = parameter(
        description="indicates that environment is production"
    ).value(False)

    conn_id = parameter(default=None, description="cloud connection settings")[str]

    # MAIN OUTPUT FOLDER
    root = parameter.folder[DirTarget]

    # DATABAND SYSTEM FOLDERS
    dbnd_root = parameter.output.folder(default=None)[DirTarget]
    dbnd_local_root = parameter.output.folder()[DirTarget]
    dbnd_data_sync_root = parameter.output.folder()[DirTarget]

    submit_engine = parameter(description="Submit engine configuration name").none[str]
    driver_engine = parameter(
        default="local_machine_engine", description="Driver engine config name"
    )[str]
    task_engine = parameter(description="Task engine configuration name").none[str]

    # properties that will affect "task-env" section
    spark_config = task_env_param.help("Spark Configuration").value("spark")

    spark_engine = task_env_param.help(
        "Cluster engine (local/emr(aws)/dataproc(gcp)/.."
    ).value("local_spark")

    hdfs = task_env_param.help("Hdfs cluster config").value("hdfs_knox")

    beam_config = task_env_param.help("Apache Beam configuration").value("beam")
    beam_engine = task_env_param.help(
        "Apache Beam cluster engine (local/dataflow)"
    ).value(ApacheBeamClusterType.local)

    docker_engine = task_env_param.help("Docker job engine (docker/aws_batch").value(
        "docker"
    )

    def _initialize(self):
        super(EnvConfig, self)._initialize()
        self.dbnd_root = self.dbnd_root or self.root.folder("dbnd")

        if not self.dbnd_local_root:
            if not self.dbnd_root.is_local():
                raise friendly_error.config.dbnd_root_local_not_defined(self.name)
            self.dbnd_local_root = self.dbnd_root

        if not self.dbnd_data_sync_root:
            self.dbnd_data_sync_root = self.dbnd_root.folder("sync")

    @property
    def name(self):
        return self.task_meta.task_name

    @property
    def cloud_type(self):
        return self.task_meta.task_family

    def prepare_env(self):
        pass


class LocalEnvConfig(EnvConfig):
    _conf__task_family = CloudType.local
