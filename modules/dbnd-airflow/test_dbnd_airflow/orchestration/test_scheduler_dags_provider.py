import logging

from dbnd import dbnd_run_cmd
from dbnd._core.configuration.scheduler_file_config_loader import (
    SchedulerFileConfigLoader,
)
from dbnd._core.utils.string_render import StringRenderer
from dbnd._core.utils.timezone import convert_to_utc
from dbnd_airflow.scheduler.scheduler_dags_provider import DbndSchedulerDBDagsProvider


logger = logging.getLogger(__name__)


def get_dags_from_file():
    reader = SchedulerFileConfigLoader()
    from_file = reader.read_config(reader.config_file)
    from_file = reader.load_and_validate(from_file)

    provider = DbndSchedulerDBDagsProvider()
    dags = []
    for job in from_file:
        import datetime

        job["start_date"] = convert_to_utc(
            job.get("start_date", datetime.datetime.now())
        )
        if job["active"]:
            dag = provider.job_to_dag(job)
            dags.append(dag)
    return dags


class TestSchedulerDagsProvider(object):
    def test_all_commands_scheduler(self):
        for i, dag in enumerate(get_dags_from_file()):
            with dag:
                cmd = dag.tasks[0].scheduled_cmd
            if not cmd.startswith("dbnd run"):
                RuntimeError(f"Invalid databand cmd found: {cmd}")

            cmd = cmd.replace("dbnd run", "")
            cmd = StringRenderer.from_str(cmd).render_str(tomorrow_ds="today")
            logger.info("Running command : %s", cmd)
            dbnd_run_cmd(args=cmd)
