import logging
import os

from dbnd._vendor import click
from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import patch_airflow_create_app
from dbnd_airflow.utils import dbnd_airflow_path


logger = logging.getLogger(__name__)


def parsedate(string):
    import pendulum

    return pendulum.parse(string, tz=pendulum.timezone("UTC"))


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("airflow_args", nargs=-1, type=click.UNPROCESSED)
def airflow(airflow_args):
    """Forward arguments to airflow command line"""

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
            # we need it right now, plugins mechanism already scanned current folder
            patch_airflow_create_app()
            os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = dbnd_airflow_path(
                "plugins", "loadable_plugins", "patched_versioned_bag"
            )

            # ANOTHER OPTION WOULD BE TO ADD PERMAMENT LINK, however it will affect all other commands
            # plugin = dbnd_airflow_path(
            #     "plugins", "dbnd_airflow_webserver_plugin_linkable.py"
            # )
            # plugin_target = os.path.join(
            #     settings.PLUGINS_FOLDER, "dbnd_airflow_webserver_plugin.py"
            # )
            # link_dropin_file(
            #     plugin, plugin_target, unlink_first=False, name="web plugin"
            # )

    from airflow.bin.cli import get_parser

    af_parser = get_parser()
    known_args = af_parser.parse_args(airflow_args)
    known_args.func(known_args)
