import logging
import subprocess
import sys
import typing

from dbnd._core.constants import TaskExecutorType
from dbnd._core.errors import DatabandConfigError
from dbnd._core.errors.friendly_error.executor_k8s import (
    local_engine_not_accept_remote_jobs,
)
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.validators import NonEmptyString
from dbnd._core.plugin.dbnd_plugins import assert_airflow_enabled, is_airflow_enabled
from dbnd._core.task import config
from dbnd._core.task_executor.local_task_executor import LocalTaskExecutor
from targets import DirTarget


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


class EngineConfig(config.Config):
    """Databand's engine (where tasks are executed)"""

    require_submit = parameter.value(False)
    task_executor_type = parameter(
        default=None,
        description="Alternate executor type: "
        " local/airflow_inprocess/airflow_multiprocess_local/airflow_kubernetes,"
        "  see docs for more options",
    )[str]

    parallel = parameter.value(False)

    sql_alchemy_conn = parameter(
        default=None,
        description="Alternate sql connection when submitting to the engine",
    )[str]

    dbnd_local_root = parameter(default=None)[DirTarget]
    dbnd_executable = parameter(
        default=[sys.executable, "-m", "dbnd"],
        description="'dbnd' executable path at engine environment",
    )[typing.List[str]]

    def cleanup_after_run(self):
        pass

    def _validate(self):
        super(EngineConfig, self)._validate()
        if self.task_executor_type is None:
            if is_airflow_enabled():
                from dbnd_airflow.executors import AirflowTaskExecutorType

                self.task_executor_type = AirflowTaskExecutorType.airflow_inprocess
            else:
                self.task_executor_type = TaskExecutorType.local

        if self.parallel:
            if is_airflow_enabled():
                from dbnd_airflow.executors import AirflowTaskExecutorType

                if self.task_executor_type == TaskExecutorType.local:
                    logger.warning(
                        "Auto switching to engine type '%s' due to parallel mode.",
                        AirflowTaskExecutorType.airflow_multiprocess_local,
                    )
                    self.task_executor_type = (
                        AirflowTaskExecutorType.airflow_multiprocess_local
                    )
                if self.task_executor_type == AirflowTaskExecutorType.airflow_inprocess:
                    logger.warning(
                        "Auto switching to engine type '%s' due to parallel mode.",
                        AirflowTaskExecutorType.airflow_multiprocess_local,
                    )
                    self.task_executor_type = (
                        AirflowTaskExecutorType.airflow_multiprocess_local
                    )
            else:
                logger.warning(
                    "Airflow is not installed, parallel mode is not supported"
                )

    def get_task_executor(self, run, host_engine, target_engine, task_runs):
        if self.task_executor_type == TaskExecutorType.local:
            return LocalTaskExecutor(run, host_engine, target_engine, task_runs)
        elif self.task_executor_type.startswith("airflow"):
            assert_airflow_enabled()
            from dbnd_airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
                AirflowTaskExecutor,
            )

            return AirflowTaskExecutor(
                run,
                host_engine=host_engine,
                target_engine=target_engine,
                task_runs=task_runs,
            )
        else:
            raise DatabandConfigError(
                "Unsupported engine type %s" % self.task_executor_type
            )

    def is_save_pipeline(self):
        if self.require_submit:
            return True

        if self.task_executor_type == TaskExecutorType.local:
            return False

        if is_airflow_enabled():
            from dbnd_airflow.executors import AirflowTaskExecutorType

            return self.task_executor_type not in [
                AirflowTaskExecutorType.airflow_inprocess,
                TaskExecutorType.local,
            ]
        return True

    def submit_to_engine_task(self, env, task_name, args):
        raise local_engine_not_accept_remote_jobs(self.env, self)

    def prepare_for_run(self, run):
        # type: (DatabandRun) -> None
        return

    def will_submit_by_executor(self):
        if self.task_executor_type == "airflow_kubernetes":
            return True
        return False


class LocalMachineEngineConfig(EngineConfig):
    _conf__task_family = "local_machine"

    def submit_to_engine_task(self, env, task_name, args):
        from dbnd.tasks.basics.shell import bash_cmd

        return bash_cmd.task(
            args=args,
            task_version="now",
            task_env=env,
            task_name=task_name,
            task_is_system=True,
        )


class ContainerEngineConfig(EngineConfig):
    require_submit = True
    dbnd_executable = ["dbnd"]  # we should have 'dbnd' command installed in container
    container_repository = parameter(validator=NonEmptyString()).help(
        "Docker container registry"
    )[str]
    container_tag = parameter.none().help("Docker container tag")[str]

    docker_build_tag = parameter.help("Auto build docker container tag").value(
        "dbnd_build"
    )
    docker_build = parameter(default=True).help(
        "Automatically build docker image. "
        "If container_repository is unset it will be taken (along with the tag) from the docker build settings"
    )[bool]
    docker_build_push = parameter(default=True).help(
        "If docker_build is enabled, controls whether the image is automatically pushed or not"
    )

    def get_docker_ctrl(self, task_run):
        pass

    @property
    def full_image(self):
        return "{}:{}".format(self.container_repository, self.container_tag)

    def prepare_for_run(self, run):
        # type: (DatabandRun) -> None
        super(ContainerEngineConfig, self).prepare_for_run(run)

        from dbnd_docker.submit_ctrl import prepare_docker_for_executor

        # when we run at submitter - we need to update driver_engine - this one will be used to send job
        # when we run at driver - we update task config, it will be used by task
        # inside pod submission the fallback is always on task_engine

        prepare_docker_for_executor(run, self)

    def submit_to_engine_task(self, env, task_name, args):

        from dbnd_docker.docker.docker_task import DockerRunTask

        submit_task = DockerRunTask(
            task_name=task_name,
            command=subprocess.list2cmdline(args),
            image=self.full_image,
            docker_engine=self,
            task_is_system=True,
        )
        return submit_task
