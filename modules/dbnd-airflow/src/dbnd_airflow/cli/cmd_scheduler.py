from __future__ import print_function

import logging
import os
import signal

from dbnd._core.configuration.environ_config import (
    ENV_DBND_QUIET,
    in_shell_cmd_complete_mode,
)
from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._vendor import click
from dbnd_airflow.utils import dbnd_airflow_path


logger = logging.getLogger(__name__)

# avoid importing airflow on autocomplete (takes approximately 1s)
if not in_shell_cmd_complete_mode():
    from airflow.bin.cli import DAGS_FOLDER
    import airflow
    from airflow.executors import LocalExecutor, SequentialExecutor
    from airflow.jobs import SchedulerJob
    from airflow.settings import SQL_ALCHEMY_CONN


def configure_airflow_scheduler_logging():
    # we need to replace current console logger with the one from airflow redirect
    # otherwise, when processor is running, any log from root Logger will not get to "airflow.processor" logger
    # as a result we will not see in the file
    from airflow.utils.log.logging_mixin import RedirectStdHandler

    airflow_console_handler = RedirectStdHandler("sys.stdout")
    dbnd_console = next((h for h in logging.root.handlers if h.name == "console"), None)
    if not dbnd_console:
        logging.info("Can not find dbnd console logger")
        return

    from airflow.settings import LOG_FORMAT

    airflow_console_handler.formatter = logging.Formatter(fmt=LOG_FORMAT)

    logging.root.removeHandler(dbnd_console)
    logging.root.addHandler(airflow_console_handler)

    # disables bootstrap prints of the launchers starting (since we still force all airflow tasks to go through dbnd airflow run ...)
    os.environ[ENV_DBND_QUIET] = "don't spam the log"


def link_dropin_dbnd_scheduled_dags(sub_dir, unlink_first=True):
    if not sub_dir:
        raise Exception("Can't link scheduler: airflow's dag folder is undefined")

    source_path = dbnd_airflow_path("scheduler", "dags", "dbnd_dropin_scheduler.py")
    target_path = os.path.join(sub_dir, "dbnd_dropin_scheduler.py")

    if unlink_first and os.path.islink(target_path):
        try:
            logger.info("unlinking existing drop-in scheduler file at %s", target_path)
            os.unlink(target_path)
        except Exception:
            logger.error(
                "failed to unlink drop-in scheduler file at %s: %s",
                (target_path, format_exception_as_str()),
            )
            return

    if not os.path.exists(target_path):
        try:
            logger.info("Linking %s to %s.", source_path, target_path)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            os.symlink(source_path, target_path)
        except Exception:
            logger.error(
                "failed to link drop-in scheduler in the airflow dags_folder: %s"
                % format_exception_as_str()
            )


@click.command()
@click.option("--dag-id")
@click.option(
    "--subdir",
    "-sd",
    default=None,
    help="File location or directory from which to look for the dag. "
    "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
    "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' ",
)
@click.option(
    "--run-duration",
    "-r",
    type=int,
    help="Set number of seconds to execute before exiting",
)
@click.option(
    "--num-runs",
    "-n",
    type=int,
    default=-1,
    help="Set the number of runs to execute before exiting",
)
@click.option(
    "--do-pickle",
    "-p",
    is_flag=True,
    help="Attempt to pickle the DAG object to send over "
    "to the workers, instead of letting workers run their version "
    "of the code.",
)
@click.option(
    "--airflow-dags-only", is_flag=True, help="Disable using DBND for scheduling dags."
)
@click.option("--pid", type=click.Path(exists=False))
@click.option("--daemon", "-d", is_flag=True)
@click.option("--stdout", type=click.Path(exists=False))
@click.option("--stderr", type=click.Path(exists=False))
@click.option("--log-file", "-l", type=click.Path(exists=False))
def scheduler(
    dag_id,
    subdir,
    run_duration,
    num_runs,
    do_pickle,
    airflow_dags_only,
    pid,
    daemon,
    stdout,
    stderr,
    log_file,
):
    """Start Databand's scheduler"""
    # We are going to show all scheduler tasks in UI for now!
    # config.set_parameter(CoreConfig.tracker, "", source="fast_dbnd_context")
    from airflow.bin.cli import (
        process_subdir,
        setup_locations,
        setup_logging,
        sigint_handler,
        sigquit_handler,
    )

    subdir = subdir or DAGS_FOLDER

    from daemon.pidfile import TimeoutPIDLockFile

    configure_airflow_scheduler_logging()

    if not airflow_dags_only:
        from airflow import settings

        link_dropin_dbnd_scheduled_dags(
            process_subdir(subdir) or settings.DAGS_FOLDER, unlink_first=True
        )

    logger = logging.getLogger(__name__)
    logger.info("Running embedded airflow scheduler")
    job = SchedulerJob(
        dag_id=dag_id,
        subdir=process_subdir(subdir),
        run_duration=run_duration,
        num_runs=num_runs,
        do_pickle=do_pickle,
    )

    if daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "scheduler", pid, stdout, stderr, log_file
        )
        handle = setup_logging(log_file)
        stdout = open(stdout, "w+")
        stderr = open(stderr, "w+")

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            job.run()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()
