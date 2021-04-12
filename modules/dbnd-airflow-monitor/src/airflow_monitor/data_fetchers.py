import json
import logging
import sys
import traceback

from urllib.parse import urlparse

import requests
import simplejson
import six

from airflow_monitor.airflow_servers_fetching import AirflowFetchingConfiguration
from airflow_monitor.common.metric_reporter import METRIC_REPORTER
from airflow_monitor.errors import (
    AirflowFetchingException,
    failed_to_connect_to_airflow_server,
    failed_to_connect_to_server_port,
    failed_to_decode_data_from_airflow,
    failed_to_fetch_from_airflow,
    failed_to_get_csrf_token,
    failed_to_login_to_airflow,
)
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
        quantity,
        fetch_type,
        incomplete_offset,
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
        self.endpoint_url = config.url + "/export_data"
        self.api_mode = config.api_mode
        self.rbac_username = config.rbac_username
        self.rbac_password = config.rbac_password
        self.client = requests.session()
        self.is_logged_in = False

    def get_data(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        quantity,
        fetch_type,
        incomplete_offset,
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
        if quantity:
            params["fetch_quantity"] = quantity
        if incomplete_offset is not None:
            params["incomplete_offset"] = incomplete_offset
        if fetch_type:
            params["fetch_type"] = fetch_type

        data = None

        try:
            data = self._make_request(params)
            logger.info("Fetched from: {}".format(data.url))
        except AirflowFetchingException:
            raise
        except requests.exceptions.ConnectionError as ce:
            raise failed_to_connect_to_airflow_server(self.base_url, ce)
        except ValueError as ve:
            raise failed_to_connect_to_server_port(self.base_url, ve)
        except Exception as e:
            raise failed_to_fetch_from_airflow(self.base_url, e)

        if data.status_code != 200:
            raise failed_to_fetch_from_airflow(self.base_url, None, data.status_code)

        try:
            json_data = data.json()
            return json_data
        except simplejson.JSONDecodeError as je:
            data_sample = data.text[:100] if data and data.text else None
            raise failed_to_decode_data_from_airflow(self.base_url, je, data_sample)

    def _try_login(self):
        login_url = self.base_url + "/login/"
        auth_params = {"username": self.rbac_username, "password": self.rbac_password}

        # IMPORTANT: when airflow uses RBAC (Flask-AppBuilder [FAB]) it doesn't return
        # the relevant csrf token in a cookie, but inside the login page html content.
        # therefore, we are extracting it, and attaching it to the session manually
        if self.api_mode == "rbac":
            # extract csrf token, will raise ConnectionError if the server is is down
            logger.info(
                "Trying to login to %s with username: %s.",
                login_url,
                self.rbac_username,
            )
            resp = self.client.get(login_url)
            soup = bs(resp.text, "html.parser")
            csrf_token_tag = soup.find(id="csrf_token")
            if not csrf_token_tag:
                raise failed_to_get_csrf_token(self.base_url)
            csrf_token = csrf_token_tag.get("value")
            if csrf_token:
                auth_params["csrf_token"] = csrf_token
            else:
                raise failed_to_get_csrf_token(self.base_url)

        # login
        resp = self.client.post(login_url, data=auth_params)

        # validate login succeeded
        soup = bs(resp.text, "html.parser")
        if "/logout/" in [a.get("href") for a in soup.find_all("a")]:
            self.is_logged_in = True
            logger.info("Succesfully logged in to %s.", login_url)
        else:
            logger.warning("Could not login to %s.", login_url)
            raise failed_to_login_to_airflow(self.base_url)

    def _make_request(self, params):
        auth = ()
        if self.api_mode == "experimental":
            auth = (self.rbac_username, self.rbac_password)
        elif self.api_mode == "rbac" and not self.is_logged_in:
            # In RBAC mode, we need to login with admin credentials first
            self._try_login()

        parsed_uri = urlparse(self.endpoint_url)
        airflow_instance_url = "{uri.scheme}://{uri.netloc}".format(uri=parsed_uri)
        with METRIC_REPORTER.exporter_response_time.labels(
            airflow_instance_url, "export_data"
        ).time():
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
        quantity,
        fetch_type,
        incomplete_offset,
    ):
        try:
            data = self.export_data_directly(
                since=since,
                include_logs=include_logs,
                include_task_args=include_task_args,
                include_xcom=include_xcom,
                dag_ids=dag_ids,
                quantity=quantity,
                fetch_type=fetch_type,
                incomplete_offset=incomplete_offset,
            )
            return data
        except Exception as ex:
            from sqlalchemy.engine.url import make_url

            logger.exception(
                "Failed to connect to db %s", repr(make_url(self.sql_conn_string)), ex
            )
            raise

    def get_source(self):
        return self.sql_conn_string

    def export_data_directly(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        quantity,
        fetch_type,
        incomplete_offset,
    ):
        from airflow import conf, models, settings
        from airflow.settings import STORE_SERIALIZED_DAGS
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from dbnd_airflow_export.plugin_old.data_exporting import get_airflow_data
        from dbnd_airflow_export.plugin_old.metrics import METRIC_COLLECTOR
        from dbnd_airflow_export.utils import JsonEncoder

        conf.set("core", "sql_alchemy_conn", value=self.sql_conn_string)
        dagbag = models.DagBag(
            self.dag_folder if self.dag_folder else settings.DAGS_FOLDER,
            include_examples=True,
            store_serialized_dags=STORE_SERIALIZED_DAGS,
        )

        engine = create_engine(self.sql_conn_string)
        session = sessionmaker(bind=engine)
        try:
            with METRIC_COLLECTOR.use_local() as metrics:
                result = get_airflow_data(
                    dagbag=dagbag,
                    since=since,
                    include_logs=include_logs,
                    include_task_args=include_task_args,
                    include_xcom=include_xcom,
                    dag_ids=dag_ids,
                    quantity=quantity,
                    fetch_type=fetch_type,
                    incomplete_offset=incomplete_offset,
                    session=session(),
                )
                result["metrics"] = {
                    "performance": metrics.get("perf_metrics", {}),
                    "sizes": metrics.get("size_metrics", {}),
                }
        except Exception:
            exception_type, exception, exc_traceback = sys.exc_info()
            message = "".join(traceback.format_tb(exc_traceback))
            message += "{}: {}. ".format(exception_type.__name__, exception)
            logging.error("Exception during data export: \n%s", message)
            result = {"error": message}

        return json.loads(json.dumps(result, cls=JsonEncoder))


class FileFetcher(DataFetcher):
    def __init__(self, config):
        # type: (AirflowFetchingConfiguration) -> FileFetcher
        super(FileFetcher, self).__init__(config)
        self.env = "JsonFile"
        self.json_file_path = config.json_file_path

    def get_data(
        self,
        since,
        include_logs,
        include_task_args,
        include_xcom,
        dag_ids,
        quantity,
        fetch_type,
        incomplete_offset,
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
        raise Exception(err)
