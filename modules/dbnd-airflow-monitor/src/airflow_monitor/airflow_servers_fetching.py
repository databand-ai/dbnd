import logging


logger = logging.getLogger(__name__)

AIRFLOW_API_MODE_TO_SUFFIX = {
    "flask-admin": "/admin/data_export_plugin/export_data",
    "rbac": "/exportdataviewappbuilder/export_data",
    "experimental": "/api/experimental/export_data",
}


class AirflowFetchingConfiguration(object):
    def __init__(
        self,
        url,
        fetcher,
        composer_client_id,
        api_mode,
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
        self.sql_alchemy_conn = sql_alchemy_conn
        self.local_dag_folder = local_dag_folder
        self.json_file_path = json_file_path
        self.rbac_username = rbac_username
        self.rbac_password = rbac_password


class AirflowServersGetter(object):
    FETCH_API_URL = "airflow_web_servers"

    def __init__(self, databand_url, api_client):
        self._fetch_url = databand_url
        self._api_client = api_client

    def get_fetching_configuration(self, airflow_config):
        try:
            response = self._api_client.api_request(
                endpoint=AirflowServersGetter.FETCH_API_URL, method="GET", data=None
            )
            result_json = response["data"]
            servers = [
                AirflowFetchingConfiguration(
                    url=server["base_url"],
                    api_mode=server["api_mode"],
                    sql_alchemy_conn=airflow_config.sql_alchemy_conn,
                    fetcher=server["fetcher"],
                    composer_client_id=server["composer_client_id"],
                    json_file_path=airflow_config.json_file_path,
                    rbac_username=airflow_config.rbac_username,
                    rbac_password=airflow_config.rbac_password,
                )
                for server in result_json
                if server["is_sync_enabled"]
            ]
            return servers
        except Exception as e:
            logger.error(
                "An error occurred while connecting to server: {}. Error: {}".format(
                    self._fetch_url, e
                )
            )
