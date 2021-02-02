import datetime
import json
import logging
import sys
import traceback

import flask
import flask_admin
import flask_appbuilder
import pendulum

from airflow.configuration import conf
from airflow.models import DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from flask import Response

from dbnd_airflow_export.dag_processing import get_current_dag_model
from dbnd_airflow_export.logic import (
    get_complete_data,
    get_dags_list_only,
    get_incomplete_data_type_1,
    get_incomplete_data_type_2,
)


class ExportDataViewAppBuilder(flask_appbuilder.BaseView):
    endpoint = "data_export_plugin"
    default_view = "export_data"

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/export_data")
    def export_data(self):
        from airflow.www_rbac.views import dagbag

        return export_data_api(dagbag)


class ExportDataViewAdmin(flask_admin.BaseView):
    def __init__(self, *args, **kwargs):
        super(ExportDataViewAdmin, self).__init__(*args, **kwargs)
        self.endpoint = "data_export_plugin"

    @flask_admin.expose("/")
    @flask_admin.expose("/export_data")
    def export_data(self):
        from airflow.www.views import dagbag

        return export_data_api(dagbag)


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
        since = pendulum.parse(str(since).replace(" 00:00", "Z"))
    else:
        since = pendulum.datetime.min

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


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        # convert dates and numpy objects in a json serializable format
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def json_response(obj):
    return Response(
        response=json.dumps(obj, indent=4, cls=JsonEncoder),
        status=200,
        mimetype="application/json",
    )


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
    rbac_enabled = conf.get("webserver", "rbac").lower() == "true"
    incomplete_offset = flask.request.args.get("incomplete_offset", type=int)
    fetch_type = flask.request.args.get("fetch_type")

    if not since and not include_logs and not dag_ids and not quantity:
        new_since = datetime.datetime.utcnow().replace(
            tzinfo=pendulum.timezone("UTC")
        ) - datetime.timedelta(days=1)
        redirect_url = (
            "ExportDataViewAppBuilder" if rbac_enabled else "data_export_plugin"
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


class DataExportAirflowPlugin(AirflowPlugin):
    name = "dbnd_airflow_export"
    admin_views = [ExportDataViewAdmin(category="Admin", name="Export Data")]
    appbuilder_views = [
        {"category": "Admin", "name": "Export Data", "view": ExportDataViewAppBuilder()}
    ]


try:
    # this import is critical for loading `requires_authentication`
    from airflow import api

    api.load_auth()

    from airflow.www_rbac.api.experimental.endpoints import (
        api_experimental,
        requires_authentication,
    )

    @api_experimental.route("/export_data", methods=["GET"])
    @requires_authentication
    def export_data():
        from airflow.www_rbac.views import dagbag

        return export_data_api(dagbag)


except Exception as e:
    logging.error("Export data could not be added to experimental api: %s", e)
