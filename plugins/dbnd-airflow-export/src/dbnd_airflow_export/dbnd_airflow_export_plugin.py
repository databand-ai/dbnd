import logging

import flask_admin
import flask_appbuilder

from airflow.plugins_manager import AirflowPlugin

from dbnd_airflow_export.plugin_old.data_exporting import export_data_api
from dbnd_airflow_export.request_processing import (
    process_dag_run_states_data_request,
    process_full_runs_request,
    process_last_seen_values_request,
    process_new_runs_request,
)


class ExportDataViewAppBuilder(flask_appbuilder.BaseView):
    endpoint = "data_export_plugin"
    default_view = "export_data"

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/export_data")
    def export_data(self):
        from airflow.www_rbac.views import dagbag

        return export_data_api(dagbag)

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/last_seen_values")
    def last_seen_values(self):
        return process_last_seen_values_request()

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/new_runs")
    def new_runs(self):
        return process_new_runs_request()

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/full_runs")
    def full_runs(self):
        return process_full_runs_request()

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/runs_states_data")
    def task_instances(self):
        return process_dag_run_states_data_request()


class ExportDataViewAdmin(flask_admin.BaseView):
    def __init__(self, *args, **kwargs):
        super(ExportDataViewAdmin, self).__init__(*args, **kwargs)
        self.endpoint = "data_export_plugin"
        self.default_view = "export_data"

    @flask_admin.expose("/")
    @flask_admin.expose("/export_data")
    def export_data(self):
        from airflow.www.views import dagbag

        return export_data_api(dagbag)

    @flask_admin.expose("/")
    @flask_admin.expose("/last_seen_values")
    def last_seen_values(self):
        return process_last_seen_values_request()

    @flask_admin.expose("/")
    @flask_admin.expose("/new_runs")
    def new_runs(self):
        return process_new_runs_request()

    @flask_admin.expose("/")
    @flask_admin.expose("/full_runs")
    def full_runs(self):
        return process_full_runs_request()

    @flask_admin.expose("/")
    @flask_admin.expose("/runs_states_data")
    def task_instances(self):
        return process_dag_run_states_data_request()


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

    @api_experimental.route("/last_seen_values", methods=["GET"])
    @requires_authentication
    def last_seen_values(self):
        return process_last_seen_values_request()

    @api_experimental.route("/new_runs", methods=["GET"])
    @requires_authentication
    def new_runs(self):
        return process_new_runs_request()

    @api_experimental.route("/full_runs", methods=["GET"])
    @requires_authentication
    def full_runs(self):
        return process_full_runs_request()

    @api_experimental.route("/runs_states_data", methods=["GET"])
    @requires_authentication
    def task_instances(self):
        return process_dag_run_states_data_request()


except Exception as e:
    logging.error("Export data could not be added to experimental api: %s", e)
