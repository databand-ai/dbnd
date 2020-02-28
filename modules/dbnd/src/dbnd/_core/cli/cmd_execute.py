import logging

from dbnd._vendor import click


logger = logging.getLogger(__name__)


@click.group()
@click.option("--dbnd-run", required=True, type=click.Path())
# option that disables the tracking store access.
@click.option("--disable-tracking-api", is_flag=True, default=False)
@click.pass_context
def execute(ctx, dbnd_run, disable_tracking_api):
    """Execute databand primitives"""
    from dbnd._core.run.databand_run import DatabandRun
    from targets import target

    run = DatabandRun.load_run(
        dump_file=target(dbnd_run), disable_tracking_api=disable_tracking_api
    )
    ctx.obj = {"run": run}


@execute.command(name="task")
@click.option("--task-id", required=True)
@click.pass_context
def run_task(ctx, task_id):
    """(Internal) Run a task inline (task.run function)"""

    with ctx.obj["run"].run_context() as dr:
        task = dr._get_task_by_id(task_id)
        task._task_run()


@execute.command(name="task_submit")
@click.option("--task-id", required=True)
@click.pass_context
def run_task_submit(ctx, task_id):
    """Submit a task"""
    with ctx.obj["run"].run_context() as dr:
        task = dr._get_task_by_id(task_id)
        task._task_submit()


@execute.command(name="task_execute")
@click.option("--task-id", required=True)
@click.pass_context
def run_task_execute(ctx, task_id):
    """Execute a task"""
    with ctx.obj["run"].run_context() as dr:
        task_run = dr.get_task_run_by_id(task_id)
        task_run.runner.execute(allow_resubmit=False)


@execute.command(name="driver")
@click.pass_context
def run_driver(ctx):
    """Run driver"""
    with ctx.obj["run"].run_context() as dr:
        # not really works
        dr.run_driver()
