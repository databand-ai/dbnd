import datetime
import logging
import re
import sys
import traceback

import flask
import pendulum

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models import DagModel
from airflow.utils.db import provide_session

from dbnd_airflow_export.compat import is_rbac_enabled
from dbnd_airflow_export.dag_operations import get_current_dag_model
from dbnd_airflow_export.datetime_utils import pendulum_min_dt
from dbnd_airflow_export.plugin_old.logic import (
    get_complete_data,
    get_dags_list_only,
    get_incomplete_data_type_1,
    get_incomplete_data_type_2,
)
from dbnd_airflow_export.utils import json_response


@provide_session
def get_airflow_data(
    dagbag,
    since,
    include_logs,
    include_task_args,
    include_xcom,
    fetch_type,
    dag_ids=None,
    quantity=None,
    incomplete_offset=None,
    session=None,
):
    if since:
        since = pendulum.parse(re.sub(r" 00:00$", "Z", str(since)))
    else:
        since = pendulum_min_dt

    # We monkey patch `get_current` to optimize sql querying
    old_get_current_dag = DagModel.get_current
    try:
        DagModel.get_current = get_current_dag_model
        if fetch_type == "dags_only":
            result = get_dags_list_only(session, dagbag, dag_ids)
        elif fetch_type == "incomplete_type1":
            result = get_incomplete_data_type_1(
                since,
                dag_ids,
                dagbag,
                quantity,
                include_task_args,
                incomplete_offset,
                session,
            )
        elif fetch_type == "incomplete_type2":
            result = get_incomplete_data_type_2(
                since,
                dag_ids,
                dagbag,
                quantity,
                include_task_args,
                incomplete_offset,
                session,
            )
        else:
            result = get_complete_data(
                since,
                dag_ids,
                dagbag,
                quantity,
                include_logs,
                include_task_args,
                include_xcom,
                session,
            )
    finally:
        DagModel.get_current = old_get_current_dag

    if result:
        result = result.as_dict()

    return result


def export_data_api(dagbag):
    since = flask.request.args.get("since")
    include_logs = flask.request.args.get("include_logs")
    include_task_args = bool(flask.request.args.get("include_task_args"))
    include_xcom = bool(flask.request.args.get("include_xcom"))
    dag_ids = (
        flask.request.args.getlist("dag_ids")
        if "dag_ids" in flask.request.args
        else None
    )
    quantity = flask.request.args.get("fetch_quantity", type=int)
    incomplete_offset = flask.request.args.get("incomplete_offset", type=int)
    fetch_type = flask.request.args.get("fetch_type")

    if not since and not include_logs and not dag_ids and not quantity:
        new_since = datetime.datetime.utcnow().replace(
            tzinfo=pendulum.timezone("UTC")
        ) - datetime.timedelta(days=1)

        redirect_url = (
            "ExportDataViewAppBuilder" if is_rbac_enabled() else "data_export_plugin"
        )
        redirect_url += ".export_data"

        return flask.redirect(flask.url_for(redirect_url, since=new_since, code=303))

    # do_update = flask.request.args.get("do_update", "").lower() == "true"
    # verbose = flask.request.args.get("verbose", str(not do_update)).lower() == "true"

    try:
        export_data = get_airflow_data(
            dagbag=dagbag,
            since=since,
            include_logs=include_logs,
            include_task_args=include_task_args,
            include_xcom=include_xcom,
            dag_ids=dag_ids,
            quantity=quantity,
            fetch_type=fetch_type,
            incomplete_offset=incomplete_offset,
        )
        export_data["metrics"] = {
            "performance": flask.g.perf_metrics,
            "sizes": flask.g.size_metrics,
        }
        logging.info("Performance metrics %s", flask.g.perf_metrics)
    except Exception:
        exception_type, exception, exc_traceback = sys.exc_info()
        message = "".join(traceback.format_tb(exc_traceback))
        message += "{}: {}. ".format(exception_type.__name__, exception)
        logging.error("Exception during data export: \n%s", message)
        export_data = {"error": message}
    return json_response(export_data)
