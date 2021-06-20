import logging
import os
import sys

from typing import Any, List, Optional, Union
from uuid import UUID

from airflow import DAG
from airflow.models import BaseOperator, DagModel

from dbnd import PythonTask, new_dbnd_context, override, parameter, task
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import (
    DBND_RESUBMIT_RUN,
    DBND_RUN_UID,
    ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD,
    in_quiet_mode,
)
from dbnd._core.configuration.scheduler_file_config_loader import (
    SchedulerFileConfigLoader,
)
from dbnd._core.constants import TaskExecutorType
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import LoggingConfig, RunConfig
from dbnd._core.tracking.no_tracking import dont_track
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.string_utils import clean_job_name
from dbnd._core.utils.timezone import convert_to_utc
from dbnd.api.scheduler import get_scheduled_jobs
from dbnd.tasks.basics.shell import bash_cmd


logger = logging.getLogger(__name__)


class DbndSchedulerDBDagsProvider(object):
    # by default only run the file sync if we are in the scheduler (and not the webserver)
    def __init__(self):
        self.scheduled_jobs = []

        if (
            config.getboolean("scheduler", "always_file_sync")
            or ("scheduler" in sys.argv)
        ) and not config.getboolean("scheduler", "never_file_sync"):
            self.file_config_loader = SchedulerFileConfigLoader()
            logger.debug("scheduler file syncing active")
        else:
            self.file_config_loader = None
            logger.debug("scheduler file syncing disabled")

        self.default_retries = config.getint("scheduler", "default_retries")

    def get_dags(self):  # type: () -> List[DAG]
        if not config.get("core", "databand_url"):
            self.scheduled_jobs = []
            return []
        logger.debug("about to get scheduler job dags from dbnd db")
        self.refresh_scheduled_jobs()
        dags = []
        for job in self.scheduled_jobs:
            if "schedule_interval" not in job:
                continue

            dag = self.job_to_dag(job)
            dag.sync_to_db()
            DagModel.get_dagmodel(dag.dag_id).set_is_paused(
                is_paused=not job["active"]
                or (
                    job.get("validation_errors", None) is not None
                    and len(job.get("validation_errors", None)) > 0
                ),
                including_subdags=False,
            )
            dags.append(dag)
        return dags

    def refresh_scheduled_jobs(self):
        changes = self.file_config_loader.sync() if self.file_config_loader else None
        if changes:
            logger.info(
                "[databand scheduler] changes in scheduler config file synced: %s"
                % ", ".join(("%s: %s" % (key, changes[key]) for key in changes))
            )

        self.scheduled_jobs = self.get_scheduled_jobs()

    def get_scheduled_jobs(self):  # type: () -> List[dict]
        return [
            s["DbndScheduledJob"]
            for s in get_scheduled_jobs()
            if not s["DbndScheduledJob"].get("validation_errors")
        ]

    def job_to_dag(self, job):  # type: (dict) -> Union[DAG, None]
        start_day = convert_to_utc(job.get("start_date", None))
        end_date = convert_to_utc(job.get("end_date", None))

        default_args = {
            "owner": job.get("create_user", None),
            "depends_on_past": job.get("depends_on_past", False),
            "start_date": job["start_date"],
            "end_date": end_date,
        }

        job_name = clean_job_name(job["name"])
        dag = DAG(
            "dbnd_launcher__%s" % job_name,
            start_date=start_day,
            default_args=default_args,
            schedule_interval=job.get("schedule_interval", None),
            catchup=job.get("catchup", False),
        )

        DbndSchedulerOperator(
            scheduled_cmd=job["cmd"],
            scheduled_job_name=job_name,
            with_name=False,
            scheduled_job_uid=job.get("uid", None),
            shell=config.getboolean("scheduler", "shell_cmd"),
            task_id="launcher",
            dag=dag,
            retries=job.get("retries", self.default_retries) or self.default_retries,
        )

        return dag


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
        **kwargs
    ):
        # type: (str, str, Optional[UUID], bool, bool, **Any) ->  DbndSchedulerOperator
        super(DbndSchedulerOperator, self).__init__(**kwargs)
        self.scheduled_job_name = scheduled_job_name
        self.scheduled_job_uid = scheduled_job_uid
        self.scheduled_cmd = scheduled_cmd
        self.shell = shell
        self.with_name = with_name

    def execute(self, context):
        scheduled_run_info = ScheduledRunInfo(
            scheduled_job_uid=self.scheduled_job_uid,
            scheduled_job_dag_run_id=context.get("dag_run").id,
            scheduled_date=context.get("task_instance").execution_date,
            scheduled_job_name=self.scheduled_job_name if self.with_name else None,
        )

        # disable heartbeat at this level,
        # otherwise scheduled jobs that will run on Kubernetes
        # will always have a heartbeat even if the actual driver
        # sent to Kubernetes is lost down the line,
        # which is the main purpose of the heartbeat
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


def get_dags():
    if environ_enabled(ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD):
        return None
    from dbnd._core.errors.base import DatabandConnectionException, DatabandApiError

    try:
        # let be sure that we are loaded
        config.load_system_configs()
        dags = DbndSchedulerDBDagsProvider().get_dags()

        if not in_quiet_mode():
            logger.info("providing %s dags from scheduled jobs" % len(dags))
        return dags
    except (DatabandConnectionException, DatabandApiError) as e:
        logger.error(str(e))
    except Exception as e:
        raise e
