import logging
import sys
import typing

from dbnd._core.constants import TaskExecutorType
from dbnd._core.errors import DatabandConfigError
from dbnd._core.errors.friendly_error.executor_k8s import (
    local_engine_not_accept_remote_jobs,
)
from dbnd._core.parameter.parameter_builder import parameter
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

    def submit_to_engine_task(self, env, task_name, args, interactive=True):
        raise local_engine_not_accept_remote_jobs(self.env, self)

    def prepare_for_run(self, run):
        # type: (DatabandRun) -> None
        return


class LocalMachineEngineConfig(EngineConfig):
    _conf__task_family = "local_machine"

    def submit_to_engine_task(self, env, task_name, args, interactive=True):
        from dbnd.tasks.basics.shell import bash_cmd

        return bash_cmd.task(
            args=args,
            task_version="now",
            task_env=env,
            task_name=task_name,
            task_is_system=True,
        )
