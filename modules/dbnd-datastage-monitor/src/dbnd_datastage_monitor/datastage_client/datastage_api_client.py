# © Copyright Databand.ai, an IBM Company 2022
import json
import logging

from abc import ABC
from http import HTTPStatus
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urljoin

import requests

from dbnd_datastage_monitor.metrics.prometheus_metrics import report_api_error
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError


logger = logging.getLogger(__name__)

CLOUD_IAM_AUTH = "cloud-iam-auth"
ON_PREM_BASIC_AUTH = "on-prem-basic-auth"
ON_PREM_IAM_AUTH = "on-prem-iam-auth"

DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 300


def raise_if_not_found(value, error_message):
    if not value:
        logger.error(error_message)
        raise ValueError(error_message)
    return value


def parse_datastage_error(response):
    try:
        errors = response.json().get("errors")
        if errors and isinstance(errors, list):
            message = "\n".join(
                [
                    f"code={error.get('code')}, message={error.get('message')}"
                    for error in errors
                ]
            )
            return message
    except Exception:
        logger.debug("Failed to parse response from %s", response.text)

    return response.text


class DataStageApiClient(ABC):
    def get_run_info(self, runs: Dict[str, str]):
        raise NotImplementedError

    def get_runs_ids(
        self, start_time: str, end_time: str, next_page: Dict[str, any] = None
    ):
        raise NotImplementedError

    def get_flow(self, job_id):
        raise NotImplementedError

    def get_run_logs(self, job_id, run_id):
        raise NotImplementedError

    def get_connections(self):
        raise NotImplementedError

    def get_job(self, job_id):
        raise NotImplementedError


class DataStageApiHttpClient(DataStageApiClient):
    DEFAULT_HOST = "https://api.dataplatform.cloud.ibm.com"
    DEFAULT_API_HOST = "https://dataplatform.cloud.ibm.com"
    DEFAULT_AUTHENTICATION_URL = "https://iam.cloud.ibm.com"

    IDENTITY_TOKEN_API_PATH = "identity/token"
    ON_PREM_TOKEN_API_PATH = "v1/preauth/validateAuth"
    ON_PREM_TOKEN_IAM_PATH = "icp4d-api/v1/authorize"
    DATASTAGE_JOBS_API_PATH = "v2/jobs"
    DATASTAGE_CAMS_API_PATH = "v2/asset_types"
    DATASTAGE_CAMS_API_ASSETS_PATH = "v2/assets"
    DATASTAGE_CONNECTIONS_API_PATH = "v2/connections"
    FLOW_API_PATH = "data_intg/v3/data_intg_flows"

    MAX_RETRIES = 3
    BACK_OFF_FACTOR = 0.3

    def __init__(
        self,
        api_key: str,
        project_id: str,
        max_retry: int = MAX_RETRIES,
        request_timeout: Union[
            float, Tuple[float, float]
        ] = DEFAULT_REQUEST_TIMEOUT_IN_SECONDS,
        page_size: int = 200,
        host_name=None,
        authentication_provider_url=None,
        authentication_type=None,
        log_exception_to_webserver=False,
    ):
        self.access_token = ""
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        self.api_key = api_key
        self.project_id = project_id
        self.retries = max_retry
        self.request_timeout = request_timeout
        self.page_size = page_size
        # hostname is provided only for datastage on-prem
        self.authentication_type = authentication_type
        self.host_name = (
            self.DEFAULT_HOST
            if self.authentication_type == CLOUD_IAM_AUTH
            else host_name
        )
        self.api_host_name = (
            self.DEFAULT_API_HOST
            if self.authentication_type == CLOUD_IAM_AUTH
            else host_name
        )
        self.authentication_provider_url = self.get_authentication_provider_url(
            authentication_provider_url
        )
        self.log_exception_to_webserver = log_exception_to_webserver

    def send_request(
        self, method, url, headers, verify, data=None, json=None, timeout=None
    ):
        return self.get_session().request(
            method=method,
            url=url,
            data=data,
            json=json,
            headers=headers,
            timeout=timeout or self.request_timeout,
            verify=verify,
        )

    def get_asset_id(self, data):
        metadata = raise_if_not_found(
            value=data.get("metadata"),
            error_message=f"Unable to add run for {self.project_id}, no metadata attribute found",
        )
        return metadata.get("asset_id")

    def build_asset_link(self, asset_id):
        return urljoin(
            self.host_name,
            f"{self.DATASTAGE_CAMS_API_ASSETS_PATH}/{asset_id}?project_id={self.project_id}",
        )

    def get_session(self):
        session = requests.Session()
        retry = Retry(
            total=self.retries,
            read=self.retries,
            connect=self.retries,
            backoff_factor=self.BACK_OFF_FACTOR,
            status_forcelist=[500, 502, 503, 504],
            method_whitelist=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_authentication_provider_url(self, authentication_url):
        if authentication_url:
            return authentication_url
        # if authentication url is not provided and on-prem, use hostname as default authentication url
        elif self.authentication_type == CLOUD_IAM_AUTH:
            return self.DEFAULT_AUTHENTICATION_URL
        else:
            return self.host_name

    def create_on_prem_token_basic_auth(self):
        logger.info("Refreshing on prem access token for project %s", self.project_id)
        url = urljoin(self.host_name, self.ON_PREM_TOKEN_API_PATH)
        headers = {
            "content-type": "application/json",
            "Authorization": f"Basic " + self.api_key,
        }
        response = self.send_request(
            method="GET", url=url, headers=headers, verify=False
        )
        response_json = response.json()
        access_token = response_json.get("accessToken")
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {self.access_token}"

    def create_iam_token_auth(self):
        logger.info("Refreshing iam access token for project %s", self.project_id)
        data = (
            f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={self.api_key}"
        )
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        url = urljoin(self.authentication_provider_url, self.IDENTITY_TOKEN_API_PATH)
        response = self.send_request(
            method="POST", url=url, data=data, headers=headers, verify=True
        )
        access_token = response.json().get("access_token")
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {self.access_token}"

    def create_on_prem_token_iam_auth(self):
        auth_headers = {"Content-Type": "application/json"}
        url = urljoin(self.authentication_provider_url, self.ON_PREM_TOKEN_IAM_PATH)
        # FIXME: why here its authentication_provider_url / get_session() but in web its authentication_provider_host / _session
        response = self.send_request(
            method="POST",
            url=url,
            data=self.api_key,
            headers=auth_headers,
            verify=False,
        )
        access_token = response.json().get("token")
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {self.access_token}"

    def refresh_access_token(self):  # TODO: FACTORY THIS
        if self.authentication_type == ON_PREM_BASIC_AUTH:
            self.create_on_prem_token_basic_auth()
        elif self.authentication_type == ON_PREM_IAM_AUTH:
            self.create_on_prem_token_iam_auth()
        else:
            self.create_iam_token_auth()

    def _make_http_request(self, method, url, body=None):
        if not self.access_token:
            self.refresh_access_token()

        ssl_verify = self.authentication_type == CLOUD_IAM_AUTH
        response = self.send_request(
            method=method,
            url=url,
            headers=self.headers,
            json=body,
            timeout=self.request_timeout,
            verify=ssl_verify,
        )
        if response.status_code == HTTPStatus.OK:
            return response.json()

        if response.status_code == HTTPStatus.UNAUTHORIZED:
            logger.info(
                "Request %s for project %s is unauthorized, probably token is expired",
                url,
                self.project_id,
            )
            self.refresh_access_token()
            response = self.send_request(
                method=method,
                url=url,
                headers=self.headers,
                json=body,
                timeout=self.request_timeout,
                verify=ssl_verify,
            )
            if response.status_code == HTTPStatus.OK:
                return response.json()

        error_message = parse_datastage_error(response)
        logger.warning(
            "Received response with status %s when trying to access url %s. Error details: %s",
            response.status_code,
            url,
            error_message,
        )
        report_api_error(response.status_code, url)
        response.raise_for_status()

    def get_runs_ids(
        self, start_time: str, end_time: str, next_page: Dict[str, any] = None
    ) -> Tuple[Dict[str, str], str]:
        runs = {}
        next_link = None
        url = urljoin(
            self.host_name,
            f"{self.DATASTAGE_CAMS_API_PATH}/job_run/search?project_id={self.project_id}",
        )
        if next_page:
            query = next_page
        else:
            query = {
                "query": f"asset.created_at: [{start_time} TO {end_time}]",
                "limit": self.page_size,
            }
        try:
            res = self._make_http_request(method="POST", url=url, body=query)
            results = res.get("results")
        except HTTPError:
            results = None

        if results:
            for re in results:
                run_id = self.get_asset_id(re)
                if not run_id:
                    continue
                run_link = self.build_asset_link(run_id)
                runs[run_id] = run_link
            next_link_result = res.get("next")
            if next_link_result:
                next_link = next_link_result
        return runs, next_link

    def filter_connection_credentials(self, connection):
        connection_entity = connection.get("entity")
        if connection_entity:
            connection_props = connection_entity.get("properties")
            if connection_props:
                filter_props = {
                    key: value
                    for key, value in connection_props.items()
                    if "password" not in key and "username" not in key
                }
                connection["entity"]["properties"] = filter_props
        return connection

    def get_connections(self):
        url = urljoin(
            self.host_name,
            f"{self.DATASTAGE_CONNECTIONS_API_PATH}?project_id={self.project_id}&userfs=false",
        )
        connections = self._make_http_request(method="GET", url=url)
        connections_result = {}
        resources = connections.get("resources")
        if not resources:
            logger.debug(
                f"Unable to add connections for {self.project_id}, no resources attribute found"
            )
        else:
            for conn in resources:
                filter_conn = self.filter_connection_credentials(conn)
                if filter_conn and filter_conn.get("metadata"):
                    connections_result[
                        filter_conn.get("metadata").get("asset_id")
                    ] = filter_conn
        return connections_result

    def get_run_info(self, run_link: str):
        run_info = self._make_http_request(method="GET", url=run_link)
        return run_info

    def get_flow(self, flow_id):
        get_flow_url = urljoin(
            self.api_host_name,
            f"{self.FLOW_API_PATH}/{flow_id}/?project_id={self.project_id}",
        )
        flow = self._make_http_request(method="GET", url=get_flow_url)
        return flow

    def get_job(self, job_id):
        job_link = self.build_asset_link(job_id)
        job_info = self._make_http_request(method="GET", url=job_link)
        return job_info

    def get_run_logs(self, job_id, run_id) -> List[Dict[str, Any]]:
        logs = []
        try:
            url = urljoin(
                self.host_name,
                f"{self.DATASTAGE_JOBS_API_PATH}/{job_id}/runs/{run_id}/logs?project_id={self.project_id}&limit={self.page_size}&userfs=false",
            )
            logs_response = self._make_http_request(method="GET", url=url)
            logs_json = logs_response.get("results")
            if logs_json:
                logs_result_len = len(logs_json)
                if logs_result_len > 1:
                    logger.exception(
                        "run id %s logs contains %s results, will not be parsed",
                        run_id,
                        logs_result_len,
                    )
                logs = json.loads(logs_json[0])
            else:
                logger.debug("Logs for run %s not found", run_id)
        except Exception as e:
            logger.exception(
                "Error occurred during fetching DataStage logs for run id: %s, Exception: %s",
                run_id,
                str(e),
            )
        finally:
            return logs
