# © Copyright Databand.ai, an IBM Company 2022

import logging
import re

from typing import Dict, List, Optional

import requests

from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.errors import (
    AirflowFetchingException,
    failed_to_connect_to_airflow_server,
    failed_to_connect_to_server_port,
    failed_to_decode_data_from_airflow,
    failed_to_fetch_from_airflow,
    failed_to_get_csrf_token,
    failed_to_login_to_airflow,
)


DEFAULT_REQUEST_TIMEOUT = 30
LONG_REQUEST_TIMEOUT = 300

DEFAULT_SESSION_TIMEOUT_IN_MINUTES = 5

logger = logging.getLogger(__name__)


# If you modify the values here, make sure to modify AIRFLOW_API_MODE_VALUES in airflow_servers_api in dbnd-web
AIRFLOW_API_MODE_TO_SUFFIX = {
    "flask-admin": "/admin/data_export_plugin",
    "rbac": "/exportdataviewappbuilder",
}

CSRF_TOKEN_PATTERN = (
    r'"csrf_token" name="csrf_token" type="hidden" value="(?P<token>.*)"'
)
LOGOUT_LINK_PATTERN = r'<a href="/logout/">'


def get_endpoint_url(base_url, api_mode):
    if api_mode.lower() not in AIRFLOW_API_MODE_TO_SUFFIX:
        raise Exception(
            "{} mode not supported. Please change your configuration to one of the following modes: {}".format(
                api_mode, ",".join(AIRFLOW_API_MODE_TO_SUFFIX.keys())
            )
        )

    suffix = AIRFLOW_API_MODE_TO_SUFFIX[api_mode.lower()]

    return base_url + suffix


class WebFetcher(AirflowDataFetcher):
    def __init__(self, config):
        # type: (AirflowServerConfig) -> None
        super(WebFetcher, self).__init__(config)
        self.env = "Airflow"
        self.base_url = config.base_url
        self.login_url = self.base_url + "/login/"

        self.rbac_username = config.rbac_username
        self.rbac_password = config.rbac_password
        self.session = requests.session()
        self.is_logged_in = False

        self._configure_api_mode_and_endpoint(config.base_url)

    def _configure_api_mode_and_endpoint(self, base_url):
        for api_mode in AIRFLOW_API_MODE_TO_SUFFIX:
            self.api_mode = api_mode
            self.endpoint_url = get_endpoint_url(base_url, api_mode)
            if self._validate_api_mode_and_endpoints():
                return

    def _validate_api_mode_and_endpoints(self):
        try:
            self.get_plugin_metadata()
            return True
        except AirflowFetchingException:
            return False

    def _get_csrf_token(self):
        # IMPORTANT: Airflow doesn't return the relevant csrf token in a cookie,
        # but inside the main page html content (In RBAC mode in the login page).
        # Therefore, we are extracting it, and attaching it to the session manually
        logger.info(
            "Trying to login to %s with username: %s.",
            self.login_url,
            self.rbac_username,
        )
        # extract csrf token, will raise ConnectionError if the server is is down
        resp = self.session.get(self.login_url)
        result = re.findall(CSRF_TOKEN_PATTERN, resp.text)
        if not result:
            raise failed_to_get_csrf_token(self.base_url)

        return result[0]

    def _login_to_server(self):
        auth_params = {"username": self.rbac_username, "password": self.rbac_password}
        csrf_token = self._get_csrf_token()

        if csrf_token:
            auth_params["csrf_token"] = csrf_token
            self.csrf_token = csrf_token
        else:
            raise failed_to_get_csrf_token(self.base_url)

        resp = self.session.post(
            self.login_url, data=auth_params, timeout=DEFAULT_REQUEST_TIMEOUT
        )

        # validate login succeeded
        logout_match = re.findall(LOGOUT_LINK_PATTERN, resp.text)
        if logout_match:
            self.is_logged_in = True
            logger.info("Succesfully logged in to %s.", self.login_url)
        else:
            logger.warning("Could not login to %s.", self.login_url)
            raise failed_to_login_to_airflow(self.base_url)

    def _make_request(
        self, endpoint_name, params, timeout=DEFAULT_REQUEST_TIMEOUT, method="POST"
    ):
        # type: (str, Dict, float, str) -> Dict

        try:
            resp = self._do_make_request(endpoint_name, params, timeout, method)
            logger.debug("Fetched from: %s", resp.url)
        except AirflowFetchingException:
            raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ce:
            raise failed_to_connect_to_airflow_server(self.base_url, ce)
        except ValueError as ve:
            raise failed_to_connect_to_server_port(self.base_url, ve)
        except Exception as e:
            raise failed_to_fetch_from_airflow(self.base_url, e)

        if resp.status_code != 200:
            msg = "endpoint: {}\nstatus: {}\n{}".format(
                resp.url, resp.status_code, resp.text
            )
            raise failed_to_fetch_from_airflow(self.base_url, None, msg)

        try:
            json_data = resp.json()
        except Exception as e:
            data_sample = resp.text[:100] if resp and resp.text else None
            raise failed_to_decode_data_from_airflow(self.base_url, e, data_sample)

        self._on_data_received(
            json_data, self.endpoint_url + "/" + endpoint_name.strip("/")
        )
        return json_data

    def _do_make_request(self, endpoint_name, params, timeout, method="POST"):
        auth = (
            (self.rbac_username, self.rbac_password)
            if self.api_mode == "experimental"
            else ()
        )

        if self.api_mode == "rbac" and not self.is_logged_in:
            # In RBAC mode, we need to login with admin credentials first
            self._login_to_server()

        if method == "GET":
            resp = self.session.get(
                self.endpoint_url + "/" + endpoint_name.strip("/"),
                params=params,
                auth=auth,
                timeout=timeout,
            )
        else:
            resp = self.session.post(
                self.endpoint_url + "/" + endpoint_name.strip("/"),
                data=params,
                auth=auth,
                timeout=timeout,
            )
        return resp

    def _raise_on_plugin_error_message(self, data, function_name):
        error_message = data.get("error_message", None)
        if error_message:
            raise AirflowFetchingException(
                "Exception occurred in function %s in Airflow: %s"
                % (function_name, error_message)
            )

    def get_source(self):
        return self.endpoint_url

    def get_last_seen_values(self) -> LastSeenValues:
        data = self._make_request("last_seen_values", {}, method="GET")
        self._raise_on_plugin_error_message(data, "get_last_seen_values")
        self._on_data_received(data, "get_last_seen_values")
        return LastSeenValues.from_dict(data)

    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int],
        last_seen_log_id: Optional[int],
        extra_dag_run_ids: Optional[List[int]],
        dag_ids: Optional[str],
    ) -> AirflowDagRunsResponse:

        params_dict = dict(
            last_seen_dag_run_id=last_seen_dag_run_id,
            last_seen_log_id=last_seen_log_id,
            include_subdags=True,
        )
        if extra_dag_run_ids:
            params_dict["extra_dag_runs_ids"] = ",".join(map(str, extra_dag_run_ids))

        if dag_ids:
            params_dict["dag_ids"] = dag_ids

        data = self._make_request("new_runs", params_dict, timeout=LONG_REQUEST_TIMEOUT)
        self._raise_on_plugin_error_message(data, "get_airflow_dagruns_to_sync")
        self._on_data_received(data, "get_airflow_dagruns_to_sync")
        return AirflowDagRunsResponse.from_dict(data)

    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        params_dict = dict(
            dag_run_ids=",".join([str(dag_run_id) for dag_run_id in dag_run_ids]),
            include_sources=include_sources,
        )
        data = self._make_request(
            "full_runs", params_dict, timeout=LONG_REQUEST_TIMEOUT
        )

        self._raise_on_plugin_error_message(data, "get_full_dag_runs")
        self._on_data_received(data, "get_full_dag_runs")
        return DagRunsFullData.from_dict(data)

    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        params_dict = {}
        if dag_run_ids:
            params_dict["dag_run_ids"] = ",".join([str(dr_id) for dr_id in dag_run_ids])

        data = self._make_request(
            "runs_states_data", params_dict, timeout=LONG_REQUEST_TIMEOUT
        )
        self._raise_on_plugin_error_message(data, "get_dag_runs_state_data")
        self._on_data_received(data, "get_dag_runs_state_data")
        return DagRunsStateData.from_dict(data)

    def is_alive(self):
        resp = self.session.get(self.base_url + "/health")
        return resp.ok

    def get_plugin_metadata(self) -> PluginMetadata:
        json_data = self._make_request("metadata", {}, method="GET")
        return PluginMetadata.from_dict(json_data)
