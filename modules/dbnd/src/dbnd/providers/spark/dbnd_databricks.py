# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os

from typing import Optional
from uuid import UUID

from dbnd._core.log import dbnd_log_exception
from dbnd._core.log.dbnd_log import dbnd_log_debug, dbnd_log_info
from dbnd._core.tracking.commands import set_external_resource_urls
from dbnd._core.utils.uid_utils import get_run_uid_from_run_id


logger = logging.getLogger(__name__)


def is_databricks_notebook_env():
    version = os.getenv("DATABRICKS_RUNTIME_VERSION")
    if version is None:
        return False
    return True


def get_databricks_notebook_context():
    from databricks.sdk.runtime import dbutils

    return dbutils.notebook.entry_point.getDbutils().notebook().getContext()


def attach_link_to_databricks_notebook():
    try:
        context = get_databricks_notebook_context()

        workspace_url = context.apiUrl().get()

        workspace_id = context.workspaceId().get()

        notebook_id = context.notebookId().get()

        notebook_url = f"{workspace_url}/?o={workspace_id}#notebook/{notebook_id}"

        set_external_resource_urls({"Databricks": notebook_url})
        dbnd_log_debug(
            f"Databricks notebook URL extracted successfully. URL: {notebook_url}"
        )

    except Exception:
        dbnd_log_exception(
            "Can't extract notebook URL from Databricks environment, exception occurred"
        )


def create_run_uid_for_databricks_notebook() -> Optional[UUID]:
    try:
        context = json.loads(get_databricks_notebook_context().toJson())
        run_id = str(context["tags"]["multitaskParentRunId"])
        run_uid = get_run_uid_from_run_id(run_id)
        dbnd_log_info(f"Databricks Notebook DBND RUN UID: {run_uid}")
        return run_uid
    except Exception:
        dbnd_log_exception(
            "Can't extract Notebook Run ID from Databricks environment, exception occurred"
        )
        return None


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
