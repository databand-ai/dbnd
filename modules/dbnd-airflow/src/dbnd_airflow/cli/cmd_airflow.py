import logging
import os

from argparse import Namespace

from dbnd._vendor import click
from dbnd_airflow.utils import dbnd_airflow_path, link_dropin_file


logger = logging.getLogger(__name__)


def parsedate(string):
    import pendulum

    return pendulum.parse(string, tz=pendulum.timezone("UTC"))


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("airflow_args", nargs=-1, type=click.UNPROCESSED)
def airflow(airflow_args):
    """Forward arguments to airflow command line"""
    from dbnd import new_dbnd_context

    if airflow_args and airflow_args[0] == "webserver":

        # Check modules
        # Print relevant error
        from airflow.plugins_manager import plugins

        plugin_found = False
        for p in plugins:
            # the class instance can come from different location ( plugins folder for example)
            from dbnd_airflow.plugins.dbnd_airflow_webserver_plugin import (
                DatabandAirflowWebserverPlugin,
            )

            if DatabandAirflowWebserverPlugin.__name__ == p.__name__:
                plugin_found = True

        if not plugin_found:
            command = "airflow webserver"
            logger.warning(
                "dbnd airflow-versioned-dag is not installed. "
                "Please run 'pip install dbnd[airflow-versioned-dag]' in order to run '{command}'.".format(
                    command=command
                )
            )
            from airflow import settings

            plugin = dbnd_airflow_path(
                "plugins", "dbnd_airflow_webserver_plugin_linkable.py"
            )
            plugin_target = os.path.join(
                settings.PLUGINS_FOLDER, "dbnd_airflow_webserver_plugin.py"
            )
            link_dropin_file(
                plugin, plugin_target, unlink_first=False, name="web plugin"
            )

    with new_dbnd_context(name="airflow", autoload_modules=False):
        from airflow.bin.cli import get_parser

        af_parser = get_parser()
        known_args = af_parser.parse_args(airflow_args)
        known_args.func(known_args)


@click.command()
@click.argument("dag_id")
@click.argument("task_id")
@click.argument("execution_date", type=parsedate)
@click.option("--dbnd-run", type=click.Path())
@click.option("--subdir", "-sd")
@click.option("--mark_success", is_flag=True)
@click.option("--force", is_flag=True)
@click.option("--pool")
@click.option("--cfg_path")
@click.option("--local", "-l", is_flag=True)
@click.option("--ignore_all_dependencies", "-A", is_flag=True)
@click.option("--ignore_depends_on_past", "-I", is_flag=True)
@click.option("--ship_dag", is_flag=True)
@click.option("--pickle", "-p")
@click.option("--job_id", "-j")
@click.option("--interactive", "-int", is_flag=True)
def run_task_airflow(dbnd_run, **run_args):
    """(Internal) Run a single task instance"""
    from dbnd._core.run.databand_run import DatabandRun
    from targets import target

    databand_run = DatabandRun.load_run(
        dump_file=target(dbnd_run), disable_tracking_api=False
    )

    with databand_run.run_context() as dr:
        # not really works
        args = Namespace(**run_args)

        task_run = dr.get_task_run(args.task_id)
        with task_run.log.capture_task_log():
            try:
                return dr.task_executor.run_airflow_task(args)
            except Exception:
                logger.exception("Failed to run %s", args.task_id)
                raise
