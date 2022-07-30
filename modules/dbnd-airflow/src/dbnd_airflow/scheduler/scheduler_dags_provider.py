# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from typing import Any, Optional
from uuid import UUID

from airflow.models import BaseOperator

from dbnd import PythonTask, new_dbnd_context, override, parameter
from dbnd._core.configuration.environ_config import DBND_RESUBMIT_RUN, DBND_RUN_UID
from dbnd._core.constants import TaskExecutorType
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import LoggingConfig, RunConfig
from dbnd._core.tracking.no_tracking import dont_track
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd.tasks.basics.shell import bash_cmd


logger = logging.getLogger(__name__)


@dont_track
class DbndSchedulerOperator(BaseOperator):
    template_fields = ("scheduled_cmd",)
    template_ext = (".sh", ".bash")
    ui_color = "#f0ede4"

    def __init__(
        self,
        scheduled_cmd,
        scheduled_job_name,
        scheduled_job_uid,
        shell,
        with_name=True,
        extra_args=None,
        **kwargs
    ):
        # type: (str, str, Optional[UUID], bool, bool, str, **Any) ->  DbndSchedulerOperator
        super(DbndSchedulerOperator, self).__init__(**kwargs)
        self.scheduled_job_name = scheduled_job_name
        self.scheduled_job_uid = scheduled_job_uid
        self.scheduled_cmd = scheduled_cmd
        self.shell = shell
        self.with_name = with_name
        self.extra_args = extra_args

    def execute(self, context):
        scheduled_run_info = ScheduledRunInfo(
            scheduled_job_uid=self.scheduled_job_uid,
            scheduled_job_dag_run_id=context.get("dag_run").id,
            scheduled_date=context.get("task_instance").execution_date,
            scheduled_job_name=self.scheduled_job_name if self.with_name else None,
            scheduled_job_extra_args=self.extra_args,
        )
        # disable heartbeat at this level,
        # otherwise scheduled jobs that will run on Kubernetes
        # will always have a heartbeat even if the actual driver
        # sent to Kubernetes is lost down the line,
        # which is the main purpose of the heartbeat

        self.launch(context, scheduled_run_info)

    def launch(self, context, scheduled_run_info: ScheduledRunInfo):
        with new_dbnd_context(
            name="airflow",
            conf={
                RunConfig.task_executor_type: override(TaskExecutorType.local),
                RunConfig.parallel: override(False),
                LoggingConfig.disabled: override(True),
            },
        ) as dc:
            launcher_task = Launcher(
                scheduled_cmd=self.scheduled_cmd,
                task_name=context.get("dag").dag_id,
                task_version="now",
                task_is_system=True,
                shell=self.shell,
            )

            dc.dbnd_run_task(
                task_or_task_name=launcher_task,
                scheduled_run_info=scheduled_run_info,
                send_heartbeat=False,
            )


class Launcher(PythonTask):
    scheduled_cmd = parameter[str]
    shell = parameter[bool]

    def run(self):
        env = os.environ.copy()
        env[DBND_RUN_UID] = str(DatabandRun.get_instance().run_uid)
        env[DBND_RESUBMIT_RUN] = "true"

        return bash_cmd.callable(
            cmd=self.scheduled_cmd, env=env, dbnd_env=False, shell=self.shell
        )
