# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.constants import ApacheBeamClusterType, CloudType, EnvLabel
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.config import Config
from targets import DirTarget


logger = logging.getLogger(__name__)

task_env_param = parameter(scope=ParameterScope.children)


class EnvConfig(Config):
    """Databand's environment configuration"""

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
    root = parameter(description="Data outputs location").folder[DirTarget]

    # DATABAND SYSTEM FOLDERS
    dbnd_root = parameter(description="DBND system outputs location").output.folder(
        default=None
    )[DirTarget]
    dbnd_local_root = parameter(
        description="DBND home for the local engine environment"
    ).output.folder()[DirTarget]
    dbnd_data_sync_root = parameter(
        description="Rooted directory for target syncing against remote engine"
    ).output.folder()[DirTarget]

    # execution
    local_engine = parameter(
        default="local_machine_engine", description="Engine for local execution"
    )[str]
    remote_engine = parameter(
        description="Remote engine for driver/tasks execution"
    ).none[str]

    submit_driver = parameter(description="Submit driver to remote_engine").none[bool]
    submit_tasks = parameter(
        description="Submit tasks to remote engine one by one"
    ).none[bool]

    # properties that will affect "task-env" section
    spark_config = task_env_param.help("Spark Configuration").value("spark")

    spark_engine = task_env_param.help(
        "Cluster engine (local/emr(aws)/dataproc(gcp)/.."
    ).value("spark_local")

    hdfs = task_env_param.help("Hdfs cluster config").value("hdfs_knox")

    beam_config = task_env_param.help("Apache Beam configuration").value("beam")
    beam_engine = task_env_param.help(
        "Apache Beam cluster engine (local/dataflow)"
    ).value(ApacheBeamClusterType.local)

    docker_engine = task_env_param.help("Docker job engine (docker/aws_batch)").value(
        "docker"
    )

    def _initialize(self):
        super(EnvConfig, self)._initialize()
        try:
            self.dbnd_root = self.dbnd_root or self.root.folder("dbnd")

            if not self.dbnd_local_root:
                if not self.dbnd_root.is_local():
                    raise friendly_error.config.dbnd_root_local_not_defined(self.name)
                self.dbnd_local_root = self.dbnd_root
        except Exception as e:
            raise friendly_error.task_build.failed_to_access_dbnd_home(
                self.dbnd_root, e
            )
        self.dbnd_local_root__build = self.dbnd_local_root.folder("build")

        if not self.dbnd_data_sync_root:
            self.dbnd_data_sync_root = self.dbnd_root.folder("sync")

        if self.submit_driver is None:
            self.submit_driver = bool(self.remote_engine)

        if self.submit_tasks is None:
            self.submit_tasks = bool(self.remote_engine)

    @property
    def name(self):
        return self.task_name

    @property
    def cloud_type(self):
        return self.task_family

    def prepare_env(self):
        pass


class LocalEnvConfig(EnvConfig):
    """
    Local environment configuration section
    """

    _conf__task_family = CloudType.local
