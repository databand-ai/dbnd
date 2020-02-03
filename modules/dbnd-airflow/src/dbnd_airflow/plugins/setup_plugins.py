from __future__ import print_function

import logging
import os

from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd_airflow.utils import dbnd_airflow_path


logger = logging.getLogger(__name__)


# avoid importing airflow on autocomplete (takes approximately 1s)


def setup_versioned_dags():
    # Check modules
    # Print relevant error
    from airflow.plugins_manager import plugins

    logging.info("Running setup for versioned dags!")
    from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import (
        patch_airflow_create_app,
    )
    from dbnd_airflow.utils import dbnd_airflow_path
    from dbnd_airflow.plugins.dbnd_airflow_webserver_plugin import (
        DatabandAirflowWebserverPlugin,
    )

    for p in plugins:
        # the class instance can come from different location ( plugins folder for example)
        if DatabandAirflowWebserverPlugin.__name__ == p.__name__:
            return True

    command = "dbnd-airflow webserver"
    logger.info(
        "dbnd-airflow-versioned-dag is not installed. "
        "Please run 'pip install dbnd-airflow-versioned-dag' in order to run '{command}'.".format(
            command=command
        )
    )
    # we need it right now, plugins mechanism already scanned current folder
    patch_airflow_create_app()
    versioned_plugin_dir = dbnd_airflow_path(
        "plugins", "loadable_plugins", "patched_versioned_bag"
    )
    logger.info(
        "Linking dag versions plugins via AIRFLOW__CORE__PLUGINS_FOLDER to %s ",
        versioned_plugin_dir,
    )
    os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = versioned_plugin_dir
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


def setup_scheduled_dags(sub_dir=None, unlink_first=True):
    from airflow import settings

    sub_dir = sub_dir or settings.DAGS_FOLDER

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
