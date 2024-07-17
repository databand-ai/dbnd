# © Copyright Databand.ai, an IBM Company 2022
import logging
import os

from dbnd._core.log import dbnd_log_exception
from dbnd._core.log.dbnd_log import dbnd_log_info


logger = logging.getLogger(__name__)


def is_databricks_notebook_env():
    version = os.getenv("DATABRICKS_RUNTIME_VERSION")
    if version is None:
        return False
    return True


def safe_get_databricks_notebook_name():
    if is_databricks_notebook_env():
        return _get_databricks_notebook_name()
    return None


def register_on_cell_exit_action(handler):
    try:
        from IPython import get_ipython

        ip = get_ipython()
        ip.events.register("post_run_cell", handler)
        dbnd_log_info(
            f"Databricks notebook on cell exit action registered successfully (action: {handler.__name__})"
        )
    except Exception:
        dbnd_log_exception(
            "Failed register post_run_cell action for databricks notebook"
        )


def unregister_on_cell_exit_action(handler):
    try:
        from IPython import get_ipython

        ip = get_ipython()
        ip.events.unregister("post_run_cell", handler)
        dbnd_log_info(
            f"Databricks notebook on cell exit action unregistered successfully (action: {handler.__name__})"
        )
    except Exception:
        dbnd_log_exception(
            "Failed unregister post_run_cell action for databricks notebook"
        )


def _get_databricks_notebook_name():
    try:
        from databricks.sdk.runtime import dbutils

        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        name = notebook_path.split("/")[-1]
        dbnd_log_info(
            f"Databricks notebook name extracted successfully. Name: {name}, Path: {notebook_path}"
        )
        return name
    except Exception:
        dbnd_log_exception(
            "Can't extract notebook name from Databricks environment, exception occurred"
        )
        return None
