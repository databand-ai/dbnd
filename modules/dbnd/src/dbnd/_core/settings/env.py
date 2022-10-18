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
    cloud_type = parameter(
        description="Set which cloud type is to be used. E.g. gcp, aws, etc."
    )[str]

    env_label = parameter(
        default=EnvLabel.dev,
        description="Set the environment type to be used. E.g. dev, int, prod.",
    )[
        str
    ]  # label

    production = parameter(
        description="This indicates that the environment is production."
    ).value(False)

    conn_id = parameter(default=None, description="Set the cloud connection settings.")[
        str
    ]

    # MAIN OUTPUT FOLDER
    root = parameter(description="Determine the main data output location.").folder[
        DirTarget
    ]

    # DATABAND SYSTEM FOLDERS
    dbnd_root = parameter(
        description="Determine DBND system output location."
    ).output.folder(default=None)[DirTarget]
    dbnd_local_root = parameter(
        description="Set DBND home for the local engine environment."
    ).output.folder()[DirTarget]
    dbnd_data_sync_root = parameter(
        description="Set rooted directory for target syncing against a remote engine."
    ).output.folder()[DirTarget]

    # execution
    local_engine = parameter(
        default="local_machine_engine",
        description="Set which engine will be used for local execution",
    )[str]
    remote_engine = parameter(
        description="Set the remote engine for the execution of driver/tasks"
    ).none[str]

    submit_driver = parameter(
        description="Enable submitting driver to `remote_engine`."
    ).none[bool]
    submit_tasks = parameter(
        description="Enable submitting tasks to remote engine one by one."
    ).none[bool]

    # properties that will affect "task-env" section
    spark_config = task_env_param.help(
        "Determine the Spark Configuration settings"
    ).value("spark")

    spark_engine = task_env_param.help(
        "Set the cluster engine to be used. E.g. local, emr (aws), dataproc (gcp), etc."
    ).value("spark_local")

    hdfs = task_env_param.help("Set the Hdfs cluster configuration settings").value(
        "hdfs_knox"
    )

    beam_config = task_env_param.help(
        "Set the Apache Beam configuration settings"
    ).value("beam")
    beam_engine = task_env_param.help(
        "Set the Apache Beam cluster engine. E.g. local or dataflow."
    ).value(ApacheBeamClusterType.local)

    docker_engine = task_env_param.help(
        "Set the Docker job engine, e.g. docker or aws_batch"
    ).value("docker")

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
