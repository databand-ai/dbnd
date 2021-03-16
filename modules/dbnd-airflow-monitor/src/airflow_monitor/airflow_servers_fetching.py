import logging

from airflow_monitor.config import AirflowMonitorConfig
from dbnd import get_databand_context
from dbnd._core.errors.base import DatabandApiError
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error


logger = logging.getLogger(__name__)

AIRFLOW_API_MODE_TO_SUFFIX = {
    "flask-admin": "/admin/data_export_plugin/export_data",
    "rbac": "/exportdataviewappbuilder/export_data",
    "experimental": "/api/experimental/export_data",
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
            response = self.fetch_airflow_servers_list()
            result_json = response["data"]
            servers = [
                AirflowFetchingConfiguration(
                    url=server["base_url"],
                    api_mode=server["api_mode"],
                    fetcher=server["fetcher"],
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
                )
                for server in result_json
                if server["is_sync_enabled"]
            ]
            return servers
        except DatabandApiError as e:
            logger.error(e)
            return None
        except Exception as e:
            msg = logger_format_for_databand_error(e)
            logger.error(msg)
            return None
