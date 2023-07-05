# © Copyright Databand.ai, an IBM Company 2022

import logging

from warnings import warn

from dbnd._core.configuration.environ_config import ENV_DBND__TRACKING
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.utils.basics.environ_utils import env as env_context
from dbnd._vendor import click
from dbnd_run.orchestration_bootstrap import dbnd_disable_databand_dags_loading
from dbnd_run.run_executor.run_executor import RunExecutor, set_active_run_context


logger = logging.getLogger(__name__)


def get_dbnd_version():
    import dbnd

    return dbnd.__version__


def get_python_version():
    import sys

    return sys.version.rsplit(" ")[0]  # string like '3.8.6'


class DbndVersionsClashWarning(UserWarning):
    pass


@click.group()
@click.option("--dbnd-run", required=True, type=click.Path())
# option that disables the tracking store access.
@click.option("--disable-tracking-api", is_flag=True, default=False)
@click.option("--expected-dbnd-version", default=None)
@click.option("--expected-python-version", default=None)
@click.pass_context
def execute(
    ctx, dbnd_run, disable_tracking_api, expected_dbnd_version, expected_python_version
):
    """Execute databand primitives"""
    dbnd_bootstrap(enable_dbnd_run=True)

    from dbnd import config

    dbnd_disable_databand_dags_loading(dbnd_config=config)

    if expected_python_version and expected_dbnd_version:
        spark_python_version = get_python_version()
        spark_dbnd_version = get_dbnd_version()
        if expected_python_version != spark_python_version:
            warn(
                "You submitted job using Python {} but the Spark cluster uses Python {}. To "
                "assure execution consistency use the same version in both places. Execution will"
                "continue but it may fail due to version mismatch.".format(
                    expected_python_version, spark_python_version
                ),
                DbndVersionsClashWarning,
            )
        if expected_dbnd_version != spark_dbnd_version:
            warn(
                "You submitted job using dbnd {} but the Spark cluster uses dbnd {}. To "
                "assure execution consistency use the same version in both places. Execution will"
                "continue but it may fail due to version mismatch.".format(
                    expected_dbnd_version, spark_dbnd_version
                ),
                DbndVersionsClashWarning,
            )

    from targets import target

    with env_context(**{ENV_DBND__TRACKING: "False"}):
        run_executor = RunExecutor.load_run(
            dump_file=target(dbnd_run), disable_tracking_api=disable_tracking_api
        )
        ctx.obj = {
            "run_executor": run_executor,
            "run": run_executor.run,
            "disable_tracking_api": disable_tracking_api,
        }


@execute.command(name="task")
@click.option("--task-id", required=True)
@click.pass_context
def run_task(ctx, task_id):
    """(Internal) Run a task inline (task.run function)"""

    run = ctx.obj["run"]  # type: DatabandRun
    with set_active_run_context(run):
        task = run._get_task_by_id(task_id)
        task_run = task.current_task_run
        # this tracking store should be the same object as the one in the context but they're actually
        # different.
        if ctx.obj["disable_tracking_api"]:
            task_run.tracker.tracking_store.disable_tracking_api()
        with task.ctrl.task_context(phase=TaskContextPhase.RUN):
            task._task_run()


@execute.command(name="task_submit")
@click.option("--task-id", required=True)
@click.pass_context
def run_task_submit(ctx, task_id):
    """Submit a task"""

    run = ctx.obj["run"]  # type: DatabandRun
    with set_active_run_context(run):
        task = run._get_task_by_id(task_id)
        task._task_submit()


@execute.command(name="task_execute")
@click.option("--task-id", required=True)
@click.pass_context
def run_task_execute(ctx, task_id):
    """Execute a task"""

    run = ctx.obj["run"]  # type: DatabandRun
    with set_active_run_context(run):
        task_run = run.get_task_run_by_id(task_id)
        task_run.task_run_executor.execute(allow_resubmit=False)
