import logging

from airflow_monitor.config import AirflowMonitorConfig
from dbnd import get_databand_context
from dbnd._core.errors.base import DatabandConfigError
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error


logger = logging.getLogger(__name__)

AIRFLOW_API_MODE_TO_SUFFIX = {
    "flask-admin": "/admin/data_export_plugin",
    "rbac": "/exportdataviewappbuilder",
    "experimental": "/api/experimental",
}

DEFAULT_FETCH_QUANTITY = 100
DEFAULT_OLDEST_INCOMPLETE_DATA_IN_DAYS = 14


class AirflowFetchingConfiguration(object):
    def __init__(
        self,
        url,
        fetcher,
        composer_client_id,
        api_mode,
        fetch_quantity,
        oldest_incomplete_data_in_days,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        sql_alchemy_conn=None,
        local_dag_folder=None,
        json_file_path=None,
        rbac_username=None,
        rbac_password=None,
        syncer_name=None,
    ):
        self.base_url = url
        self.api_mode = api_mode

        if api_mode.lower() not in AIRFLOW_API_MODE_TO_SUFFIX:
            raise Exception(
                "{} mode not supported. Please change your configuration to one of the following modes: {}".format(
                    api_mode, ",".join(AIRFLOW_API_MODE_TO_SUFFIX.keys())
                )
            )

        suffix = AIRFLOW_API_MODE_TO_SUFFIX[api_mode.lower()]

        self.url = url + suffix
        self.fetcher = fetcher
        self.composer_client_id = composer_client_id
        self.fetch_quantity = fetch_quantity
        self.oldest_incomplete_data_in_days = oldest_incomplete_data_in_days
        self.include_logs = include_logs
        self.include_task_args = include_task_args
        self.include_xcom = include_xcom
        self.dag_ids = dag_ids
        self.sql_alchemy_conn = sql_alchemy_conn
        self.local_dag_folder = local_dag_folder
        self.json_file_path = json_file_path
        self.rbac_username = rbac_username
        self.rbac_password = rbac_password
        self.syncer_name = syncer_name


class AirflowServersGetter(object):
    FETCH_API_URL = "airflow_web_servers"

    def __init__(self):
        context = get_databand_context()
        self._fetch_url = context.settings.core.databand_url
        self._api_client = context.databand_api_client

    def fetch_airflow_servers_list(self):
        response = self._api_client.api_request(
            endpoint=AirflowServersGetter.FETCH_API_URL, method="GET", data=None
        )
        return response

    def get_fetching_configurations(self):
        try:
            airflow_config = AirflowMonitorConfig()
            if airflow_config.sql_alchemy_conn and not airflow_config.syncer_name:
                raise DatabandConfigError(
                    "Syncer name should be specified when using direct sql connection",
                    help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                    " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
                )
            response = self.fetch_airflow_servers_list()
            result_json = response["data"]
            if airflow_config.syncer_name:
                results_by_name = {r["name"]: r for r in result_json if r.get("name")}
                if airflow_config.syncer_name in results_by_name:
                    result_json = [results_by_name[airflow_config.syncer_name]]
                else:
                    raise DatabandConfigError(
                        "No syncer configuration found matching name '%s'. Available syncers: %s"
                        % (
                            airflow_config.syncer_name,
                            ",".join(results_by_name.keys()),
                        ),
                        help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                        " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
                    )
            else:
                result_json = [r for r in result_json if r.get("fetcher") != "db"]
            servers = [
                AirflowFetchingConfiguration(
                    url=server["base_url"],
                    api_mode=server["api_mode"],
                    fetcher=airflow_config.fetcher or server["fetcher"],
                    composer_client_id=server["composer_client_id"],
                    fetch_quantity=int(server["fetch_quantity"])
                    if server["fetch_quantity"]
                    else DEFAULT_FETCH_QUANTITY,
                    oldest_incomplete_data_in_days=int(
                        server["oldest_incomplete_data_in_days"]
                    )
                    if server["oldest_incomplete_data_in_days"]
                    else DEFAULT_OLDEST_INCOMPLETE_DATA_IN_DAYS,
                    include_logs=bool(server["include_logs"]),
                    include_task_args=bool(server["include_task_args"]),
                    include_xcom=bool(server["include_xcom"]),
                    dag_ids=server["dag_ids"].split(",") if server["dag_ids"] else None,
                    sql_alchemy_conn=airflow_config.sql_alchemy_conn,
                    json_file_path=airflow_config.json_file_path,
                    rbac_username=airflow_config.rbac_username,
                    rbac_password=airflow_config.rbac_password,
                    syncer_name=server.get("name"),
                )
                for server in result_json
                if server["is_sync_enabled"]
                and not server.get("is_sync_enabled_v2", False)
            ]
            return servers
        except Exception as e:
            msg = logger_format_for_databand_error(e)
            logger.error(msg)
            return None
