import logging

from typing import Dict, List, Optional

import requests

from airflow_monitor.airflow_monitor_utils import log_received_tasks, send_metrics
from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
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
from bs4 import BeautifulSoup as bs


logger = logging.getLogger(__name__)


AIRFLOW_API_MODE_TO_SUFFIX = {
    "flask-admin": "/admin/data_export_plugin",
    "rbac": "/exportdataviewappbuilder",
    "experimental": "/api/experimental",
}


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
        self.api_mode = config.api_mode
        self.endpoint_url = get_endpoint_url(config.base_url, config.api_mode)
        self.rbac_username = config.rbac_username
        self.rbac_password = config.rbac_password
        self.client = requests.session()
        self.is_logged_in = False

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

    def _make_request(self, endpoint_name, params):
        # type: (str, Dict) -> Dict
        auth = ()
        if self.api_mode == "experimental":
            auth = (self.rbac_username, self.rbac_password)
        elif self.api_mode == "rbac" and not self.is_logged_in:
            # In RBAC mode, we need to login with admin credentials first
            self._try_login()

        try:
            resp = self.client.get(
                self.endpoint_url + "/" + endpoint_name.strip("/"),
                params=params,
                auth=auth,
            )
            logger.info("Fetched from: {}".format(resp.url))
        except AirflowFetchingException:
            raise
        except requests.exceptions.ConnectionError as ce:
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

        if "error" in json_data:
            logger.error("Error in Airflow Export Plugin: \n%s", json_data["error"])
            raise AirflowFetchingException(json_data["error"])

        log_received_tasks(self.base_url, json_data)
        send_metrics(
            self.base_url, json_data.get("airflow_export_meta"),
        )
        return json_data

    def get_source(self):
        return self.endpoint_url

    def get_last_seen_values(self) -> LastSeenValues:
        data = self._make_request("last_seen_values", {})
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
        )
        if extra_dag_run_ids:
            params_dict["extra_dag_runs_ids"] = ",".join(map(str, extra_dag_run_ids))

        if dag_ids:
            params_dict["dag_ids"] = dag_ids

        data = self._make_request("new_runs", params_dict)
        return AirflowDagRunsResponse.from_dict(data)

    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        params_dict = dict(
            dag_run_ids=",".join([str(dag_run_id) for dag_run_id in dag_run_ids]),
            include_sources=include_sources,
        )
        data = self._make_request("full_runs", params_dict)
        return DagRunsFullData.from_dict(data)

    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        params_dict = {}
        if dag_run_ids:
            params_dict["dag_run_ids"] = ",".join([str(dr_id) for dr_id in dag_run_ids])
        data = self._make_request("runs_states_data", params_dict)
        return DagRunsStateData.from_dict(data)

    def is_alive(self):
        resp = self.client.get(self.base_url + "/health")
        # TODO: should we actually check the response status?
        return resp.ok
