# © Copyright Databand.ai, an IBM Company 2022

from airflow.task.task_runner.standard_task_runner import StandardTaskRunner

from dbnd_airflow.constants import AIRFLOW_VERSION_2


class DbndStandardTaskRunner(StandardTaskRunner):
    def __init__(self, local_task_job):
        """
        This is to get rid of exception inside airflow.cli.commands.task_command.task_run
        Since 2.0 it will throw exception "You cannot use the --pickle option when using DAG.cli() method."

        Aiflow logs are also not helpful when this happens, the only thing in log is:
          $> Running <TaskInstance: dbnd_sanity_check backfill_{datetime} [failed]> on host ...

        This error happens inside the forked process of executor, and the only way to debug for was
          to put remote debug breakpoint in StandardTaskRunner._start_by_fork
        """
        if AIRFLOW_VERSION_2:
            # Dag is already loaded from database, and we are in forked process. Do not need to unpickle again
            local_task_job.pickle_id = None
        super().__init__(local_task_job)
