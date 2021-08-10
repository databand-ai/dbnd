# dbnd should be first - we need to setup things!
import logging

from dbnd import dbnd_bootstrap


logger = logging.getLogger(__name__)


def main():
    dbnd_bootstrap()

    from dbnd._core.task_run.task_run import TaskRun

    from airflow.models import TaskInstance
    from airflow.utils.timezone import utcnow

    from airflow.utils.log.logging_mixin import redirect_stdout, redirect_stderr
    from dbnd import config
    from dbnd._core.constants import TaskExecutorType
    from dbnd._core.settings import RunConfig
    from dbnd.tasks.basics import dbnd_sanity_check
    from dbnd_examples.dbnd_airflow import bash_dag

    airflow_task_log = logging.getLogger("airflow.task")

    task = bash_dag.t3
    execution_date = utcnow()
    ti = TaskInstance(task, execution_date)

    ti.init_run_context(raw=False)
    logger.warning(
        "Running with task_log %s", airflow_task_log.handlers[0].handler.baseFilename
    )
    with redirect_stdout(airflow_task_log, logging.INFO), redirect_stderr(
        airflow_task_log, logging.WARN
    ):
        logger.warning("from redirect")

        logger.warning("after patch")
        with config({RunConfig.task_executor_type: TaskExecutorType.local}):
            run = dbnd_sanity_check.dbnd_run()
            tr = run.root_task_run  # type: TaskRun
        pass

    logger.warning("TR: %s %s %s", tr, tr.task_tracker_url, tr.log.local_log_file)


if __name__ == "__main__":
    main()
