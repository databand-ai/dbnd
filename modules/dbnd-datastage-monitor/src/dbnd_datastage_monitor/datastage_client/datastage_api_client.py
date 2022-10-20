# © Copyright Databand.ai, an IBM Company 2022
import json
import logging

from abc import ABC
from http import HTTPStatus
from typing import Dict, Tuple

import jwt
import requests

from requests.adapters import HTTPAdapter, Retry


logger = logging.getLogger(__name__)


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
        page_size: int = 200,
        host_name=None,
        authentication_provider_url=None,
    ):
        self.access_token = ""
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        self.api_key = api_key
        self.project_id = project_id
        self.retries = max_retry
        self.page_size = page_size
        self.host_name = host_name if host_name else self.DEFAULT_HOST
        self.api_host_name = host_name if host_name else self.DEFAULT_API_HOST
        self.authentication_provider_url = (
            authentication_provider_url
            if authentication_provider_url
            else self.DEFAULT_AUTHENTICATION_URL
        )

    def validate_token(func):
        def wrapper(self, method, url, body=None):
            if not self.access_token:
                self.refresh_access_token()
            else:
                try:
                    jwt.decode(self.access_token, options={"verify_signature": False})
                except jwt.ExpiredSignatureError:
                    logger.error("access token is expired, refreshing token")
                    self.refresh_access_token()
            if body:
                return func(self, method, url, body)
            return func(self, method, url)

        return wrapper

    def get_session(self):
        session = requests.Session()
        retry = Retry(
            total=self.retries,
            read=self.retries,
            connect=self.retries,
            backoff_factor=self.BACK_OFF_FACTOR,
            status_forcelist=[500, 502, 503, 504],
            method_whitelist=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def init_http_client_session(self, retries, back_off_factor=BACK_OFF_FACTOR):
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=back_off_factor,
            method_whitelist=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def refresh_access_token(self):
        data = (
            f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={self.api_key}"
        )
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = self.get_session().post(
            f"{self.authentication_provider_url}/{self.IDENTITY_TOKEN_API_PATH}",
            data=data,
            headers=headers,
        )
        response_json = response.json()
        access_token = response_json.get("access_token")
        self.access_token = access_token
        self.headers["Authorization"] = f"Bearer {self.access_token}"

    @validate_token
    def _make_http_request(self, method, url, body=None):
        response = self.get_session().request(
            method=method, url=url, headers=self.headers, json=body
        )
        if response:
            if response.status_code == HTTPStatus.UNAUTHORIZED:
                self.refresh_access_token()
                response = self.get_session().request(
                    method=method, url=url, headers=self.headers, json=body
                )
            if response.status_code == HTTPStatus.OK:
                return response.json()
            else:
                logger.error(f"http response status code {response.status_code}")
        # return empty object to prevent NONE errors
        return {}

    def get_runs_ids(
        self, start_time: str, end_time: str, next_page: Dict[str, any] = None
    ) -> Tuple[Dict[str, str], str]:
        next_link = None
        url = f"{self.host_name}/{self.DATASTAGE_CAMS_API_PATH}/job_run/search?project_id={self.project_id}"
        if next_page:
            query = next_page
        else:
            query = {
                "query": f"asset.created_at: [{start_time} TO {end_time}]",
                "limit": self.page_size,
            }
        res = self._make_http_request(method="POST", url=url, body=query)
        runs = {}
        results = res.get("results")
        if results:
            for re in results:
                metadata = re.get("metadata")
                if not metadata:
                    logger.debug(
                        f"Unable to add run for {self.project_id}, no metadata attribute found"
                    )
                    continue
                run_id = re.get("metadata").get("asset_id")
                link = re.get("href")
                runs[run_id] = link
            next_link_result = res.get("next")
            if next_link_result:
                next_link = next_link_result
        return runs, next_link

    def get_run_info_jobs_api(self, job_id: str, run_id: str):
        url = f"{self.host_name}/{self.DATASTAGE_JOBS_API_PATH}/{job_id}/runs/{run_id}?project_id={self.project_id}&limit={self.page_size}&userfs=false"
        return self._make_http_request(method="GET", url=url)

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
        url = f"{self.host_name}/{self.DATASTAGE_CONNECTIONS_API_PATH}?project_id={self.project_id}&userfs=false"
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
        get_flow_url = f"{self.api_host_name}/{self.FLOW_API_PATH}/{flow_id}/?project_id={self.project_id}"
        flow = self._make_http_request(method="GET", url=get_flow_url)
        return flow

    def get_job(self, job_id):
        url = f"{self.host_name}/{self.DATASTAGE_CAMS_API_ASSETS_PATH}/{job_id}?project_id={self.project_id}"
        job = self._make_http_request(method="GET", url=url)
        job_info = self._make_http_request(method="GET", url=job.get("href"))
        return job_info

    def get_run_logs(self, job_id, run_id):
        url = f"{self.host_name}/{self.DATASTAGE_JOBS_API_PATH}/{job_id}/runs/{run_id}/logs?project_id={self.project_id}&limit={self.page_size}&userfs=false"
        logs = self._make_http_request(method="GET", url=url)
        logs_json = logs.get("results")
        if logs_json:
            logs = json.loads(logs_json[0])
        else:
            logger.debug(f"Logs for {run_id} not found")
        return logs
