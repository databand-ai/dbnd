import logging

import prometheus_client
import requests
import six

from bs4 import BeautifulSoup as bs


if six.PY3:
    from json import JSONDecodeError
else:
    from simplejson import JSONDecodeError

logger = logging.getLogger(__name__)


class DataFetcher(object):
    def __init__(self, config):
        self._config = config
        self.env = None

    def get_data(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        task_quantity,
    ):
        pass

    def get_source(self):
        pass


class WebFetcher(DataFetcher):
    # Common instance of prometheus summary object for all fetchers
    prometheus_af_response_time_metrics = None

    def __init__(self, config):
        # type: (AirflowFetchingConfiguration) -> WebFetcher
        super(WebFetcher, self).__init__(config)
        self.env = "Airflow"
        self.base_url = config.base_url
        self.endpoint_url = config.url
        self.rbac_enabled = config.rbac_enabled
        self.rbac_username = config.rbac_username
        self.rbac_password = config.rbac_password
        self.use_experimental_api = config.use_experimental_api
        self.client = requests.session()
        self.is_rbac_logged_in = False

        if WebFetcher.prometheus_af_response_time_metrics is None:
            WebFetcher.prometheus_af_response_time_metrics = prometheus_client.Summary(
                "af_monitor_export_response_time", "Airflow export plugin response time"
            )

    def get_data(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        task_quantity,
    ):
        params = {}
        if since:
            params["since"] = since.isoformat()
        if include_logs:
            params["include_logs"] = True
        if include_task_args:
            params["include_task_args"] = True
        if include_xcom:
            params["include_xcom"] = True
        if dag_ids:
            params["dag_ids"] = dag_ids
        if task_quantity:
            params["tasks"] = task_quantity

        try:
            data = self._make_request(params)
            logger.info("Fetched from: {}".format(data.url))
            if data.status_code == 200:
                try:
                    return data.json()
                except JSONDecodeError:
                    if data.text:
                        logger.info("Failed to decode: %s...", data.text[:100])
                    raise
            else:
                logger.error(
                    "Could not fetch data from url {}, error code: {}. Hint: If the IP address is correct"
                    " but the full path is not, check the configuration of rbac_enabled variable".format(
                        self.endpoint_url, data.status_code,
                    ),
                )
        except ConnectionError as e:
            logger.error(
                "An error occurred while connecting to server: {}. Error: {}".format(
                    self.endpoint_url, e
                )
            )

    def _try_login(self):
        login_url = self.base_url + "/login/"
        auth_params = {"username": self.rbac_username, "password": self.rbac_password}

        # IMPORTANT: when airflow uses RBAC (Flask-AppBuilder [FAB]) it doesn't return
        # the relevant csrf token in a cookie, but inside the login page html content.
        # therefore, we are extracting it, and attaching it to the session manually
        try:
            # extract csrf token
            logger.info(
                "Trying to login to %s with username: %s.",
                login_url,
                self.rbac_username,
            )
            resp = self.client.get(login_url)
            soup = bs(resp.text, "html.parser")
            csrf_token = soup.find(id="csrf_token").get("value")
            if csrf_token:
                auth_params["csrf_token"] = csrf_token
        except Exception as e:
            logger.warning("Could not collect csrf token from %s. %s", login_url, e)

        # login
        resp = self.client.post(login_url, data=auth_params)

        # validate login succeeded
        soup = bs(resp.text, "html.parser")
        if "/logout/" in [a.get("href") for a in soup.find_all("a")]:
            self.is_rbac_logged_in = True
            logger.info("Succesfully logged in to %s.", login_url)
        else:
            logger.warning("Could not login to %s.", login_url)

    def _make_request(self, params):
        auth = ()
        if self.use_experimental_api:
            if not self.rbac_enabled:
                logger.warning(
                    "Experimental api is supported only when rbac is enabled. "
                    "We will assume rbac is enabled and continue."
                )
            auth = (self.rbac_username, self.rbac_password)

        elif self.rbac_enabled and not self.is_rbac_logged_in:
            # In RBAC mode, we need to login with admin credentials first
            self._try_login()

        with WebFetcher.prometheus_af_response_time_metrics.time():
            return self.client.get(self.endpoint_url, params=params, auth=auth)

    def get_source(self):
        return self.endpoint_url


class GoogleComposerFetcher(WebFetcher):
    # requires GOOGLE_APPLICATION_CREDENTIALS env variable
    def __init__(self, config):
        # type: (AirflowFetchingConfiguration) -> GoogleComposerFetcher
        super(GoogleComposerFetcher, self).__init__(config)
        self.client_id = config.composer_client_id
        self.env = "GoogleCloudComposer"

    def _make_request(self, params):
        from airflow_monitor.make_iap_request import make_iap_request

        resp = make_iap_request(
            url=self.endpoint_url, client_id=self.client_id, params=params
        )
        return resp


class DbFetcher(DataFetcher):
    def __init__(self, config):
        # type: (AirflowFetchingConfiguration) -> DbFetcher
        super(DbFetcher, self).__init__(config)

        from sqlalchemy import create_engine

        self.dag_folder = config.local_dag_folder
        self.sql_conn_string = config.sql_alchemy_conn
        self.engine = create_engine(self.sql_conn_string)
        self.env = "AirflowDB"

    def get_data(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        task_quantity,
    ):
        from dbnd_airflow_export.dbnd_airflow_export_plugin import export_data_directly

        try:
            data = export_data_directly(
                sql_alchemy_conn=self.sql_conn_string,
                dag_folder=self.dag_folder,
                since=since,
                include_logs=include_logs,
                include_task_args=include_task_args,
                include_xcom=include_xcom,
                dag_ids=dag_ids,
                task_quantity=task_quantity,
            )
            return data
        except Exception as ex:
            logger.exception("Failed to connect to db %s", self.sql_conn_string, ex)
            raise

    def get_source(self):
        return self.sql_conn_string


class FileFetcher(DataFetcher):
    def __init__(self, config):
        # type: (AirflowFetchingConfiguration) -> FileFetcher
        super(FileFetcher, self).__init__(config)
        self.env = "JsonFile"
        self.json_file_path = config.json_file_path

    def get_data(
        self, since, include_logs, include_task_args, include_xcom, dag_ids, tasks
    ):
        import json

        if not self.json_file_path:
            raise Exception(
                "'json_file_path' was not set in AirflowMonitor configuration."
            )

        try:
            with open(self.json_file_path) as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.error(
                "Could not read json file {}. Error: {}".format(self.json_file_path, e)
            )

    def get_source(self):
        return self.json_file_path


def data_fetcher_factory(config):
    # type: (AirflowFetchingConfiguration) -> DataFetcher
    if config.fetcher == "db":
        return DbFetcher(config)
    elif config.fetcher == "web":
        return WebFetcher(config)
    elif config.fetcher == "composer":
        return GoogleComposerFetcher(config)
    elif config.fetcher == "file":
        return FileFetcher(config)

    else:
        err = "Unsupported fetcher_type: {}, use one of the following: web/db/composer/file".format(
            config.fetcher
        )
        logging.error(err)
        raise ConnectionError(err)
