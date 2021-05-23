import logging

import airflow
import flask_admin
import flask_appbuilder

from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf as admin_csrf

from dbnd_airflow_export.plugin_old.data_exporting import export_data_api
from dbnd_airflow_export.request_processing import (
    process_dag_run_states_data_request,
    process_full_runs_request,
    process_last_seen_values_request,
    process_metadata_request,
    process_new_runs_request,
)
from dbnd_airflow_export.utils import AIRFLOW_VERSION_2


if not AIRFLOW_VERSION_2:
    from airflow.www_rbac.app import csrf as rbac_csrf
else:
    from airflow.www.app import csrf as rbac_csrf


def get_dagbag_model():
    if AIRFLOW_VERSION_2:
        from airflow.models.dagbag import DagBag

        dagbag = DagBag()
    elif airflow.settings.RBAC:
        from airflow.www_rbac.views import dagbag
    else:
        from airflow.www.views import dagbag
    return dagbag


class ExportDataViewAppBuilder(flask_appbuilder.BaseView):
    endpoint = "data_export_plugin"
    default_view = "export_data"

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/export_data")
    def export_data(self):
        dagbag = get_dagbag_model()
        return export_data_api(dagbag)

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/last_seen_values")
    def last_seen_values(self):
        return process_last_seen_values_request()

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/new_runs", methods=["GET", "POST"])
    def new_runs(self):
        return process_new_runs_request()

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/full_runs", methods=["GET", "POST"])
    def full_runs(self):
        return process_full_runs_request()

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/runs_states_data", methods=["GET", "POST"])
    def task_instances(self):
        return process_dag_run_states_data_request()

    @rbac_csrf.exempt
    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/metadata")
    def metadata(self):
        return process_metadata_request()


class ExportDataViewAdmin(flask_admin.BaseView):
    def __init__(self, *args, **kwargs):
        super(ExportDataViewAdmin, self).__init__(*args, **kwargs)
        self.endpoint = "data_export_plugin"
        self.default_view = "export_data"

    @admin_csrf.exempt
    @flask_admin.expose("/")
    @flask_admin.expose("/export_data")
    def export_data(self):
        dagbag = get_dagbag_model()
        return export_data_api(dagbag)

    @admin_csrf.exempt
    @flask_admin.expose("/last_seen_values")
    def last_seen_values(self):
        return process_last_seen_values_request()

    @admin_csrf.exempt
    @flask_admin.expose("/new_runs", methods=["GET", "POST"])
    def new_runs(self):
        return process_new_runs_request()

    @admin_csrf.exempt
    @flask_admin.expose("/full_runs", methods=["GET", "POST"])
    def full_runs(self):
        return process_full_runs_request()

    @admin_csrf.exempt
    @flask_admin.expose("/runs_states_data", methods=["GET", "POST"])
    def task_instances(self):
        return process_dag_run_states_data_request()

    @admin_csrf.exempt
    @flask_admin.expose("/metadata")
    def metadata(self):
        return process_metadata_request()


class DataExportAirflowPlugin(AirflowPlugin):
    name = "dbnd_airflow_export"
    admin_views = [ExportDataViewAdmin(category="Admin", name="Export Data")]
    appbuilder_views = [
        {"category": "Admin", "name": "Export Data", "view": ExportDataViewAppBuilder()}
    ]


if not AIRFLOW_VERSION_2:
    try:
        # this import is critical for loading `requires_authentication`
        from airflow import api

        api.load_auth()
        if AIRFLOW_VERSION_2:
            from airflow.www.api.experimental.endpoints import (
                api_experimental,
                requires_authentication,
                csrf,
            )
        else:
            from airflow.www_rbac.api.experimental.endpoints import (
                api_experimental,
                requires_authentication,
                csrf,
            )

        @csrf.exempt
        @api_experimental.route("/export_data", methods=["GET"])
        @requires_authentication
        def export_data():
            dagbag = get_dagbag_model()
            return export_data_api(dagbag)

        @csrf.exempt
        @api_experimental.route("/last_seen_values", methods=["GET"])
        @requires_authentication
        def last_seen_values():
            return process_last_seen_values_request()

        @csrf.exempt
        @api_experimental.route("/new_runs", methods=["GET", "POST"])
        @requires_authentication
        def new_runs():
            return process_new_runs_request()

        @csrf.exempt
        @api_experimental.route("/full_runs", methods=["GET", "POST"])
        @requires_authentication
        def full_runs():
            return process_full_runs_request()

        @csrf.exempt
        @api_experimental.route("/runs_states_data", methods=["GET", "POST"])
        @requires_authentication
        def task_instances():
            return process_dag_run_states_data_request()

        @csrf.exempt
        @api_experimental.route("/metadata", methods=["GET"])
        @requires_authentication
        def metadata():
            return process_metadata_request()

    except Exception as e:
        logging.error("Export data could not be added to experimental api: %s", e)
