import logging

from typing import List, Optional

from airflow import DAG

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import (
    ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD,
    in_quiet_mode,
)
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.string_utils import clean_job_name
from dbnd._core.utils.timezone import convert_to_utc
from dbnd.api.scheduler import get_scheduled_jobs
from dbnd_airflow.scheduler.scheduler_dags_provider import DbndSchedulerOperator


logger = logging.getLogger(__name__)


class DbndSchedulerDBDagsProvider(object):
    # by default only run the file sync if we are in the scheduler (and not the webserver)
    def __init__(
        self, default_retries: int, custom_operator_class: Optional[type] = None
    ):
        self.custom_operator_class = custom_operator_class
        self.default_retries = default_retries

    def get_dags(self):  # type: () -> List[DAG]
        logger.debug("about to get scheduler job dags from dbnd db")
        dags = []
        jobs = get_scheduled_jobs()
        validated_jobs = [
            s["DbndScheduledJob"]
            for s in jobs
            if not s["DbndScheduledJob"].get("validation_errors")
        ]

        for job in validated_jobs:
            if "schedule_interval" not in job:
                continue
            dag = self.job_to_dag(job)
            dags.append(dag)
        return dags

    def job_to_dag(self, job):  # type: (dict) -> Union[DAG, None]

        # convert_to_utc usage might be dangerous, as there is the same function at airflow
        # however, that one use pendulum not from _vendorized

        default_args = {}
        if job.get("depends_on_past"):
            default_args["depends_on_past"] = job.get("depends_on_past")

        start_date = convert_to_utc(job.get("start_date"))
        if start_date:
            default_args["start_day"] = start_date

        if job.get("end_date"):
            default_args["end_date"] = convert_to_utc(job.get("end_date"))

        if job.get("owner"):
            default_args["owner"] = job.get("create_user")

        job_name = clean_job_name(job["name"])
        dag = DAG(
            "%s" % job_name,
            start_date=start_date,
            default_args=default_args,
            schedule_interval=job.get("schedule_interval", None),
            catchup=job.get("catchup", False),
        )

        custom_operator_class = self.custom_operator_class or DbndSchedulerOperator
        custom_operator_class(
            scheduled_cmd=job["cmd"],
            scheduled_job_name=job_name,
            extra_args=job.get("extra_args", None),
            with_name=False,
            scheduled_job_uid=job.get("uid", None),
            shell=config.getboolean("scheduler", "shell_cmd"),
            task_id="launcher",
            dag=dag,
            retries=job.get("retries") or self.default_retries,
        )

        return dag


def get_dags_from_databand(custom_operator_class: Optional[type] = None):
    if environ_enabled(ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD):
        return None
    from dbnd._core.errors.base import DatabandApiError, DatabandConnectionException

    try:

        # let be sure that we are loaded
        config.load_system_configs()
        if not config.get("core", "databand_url"):
            return {}

        default_retries = config.getint("scheduler", "default_retries")

        dags = DbndSchedulerDBDagsProvider(
            default_retries=default_retries, custom_operator_class=custom_operator_class
        ).get_dags()

        if not in_quiet_mode():
            logger.info("providing %s dags from scheduled jobs" % len(dags))
        return {dag.dag_id: dag for dag in dags}
    except (DatabandConnectionException, DatabandApiError) as e:
        logger.error(str(e))
        raise e
    except Exception as e:
        logging.exception("Failed to get dags form databand server")
        raise e
