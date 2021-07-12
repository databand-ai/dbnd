import logging

from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Optional, Tuple, Union

import requests

from six.moves.urllib_parse import urljoin

from dbnd._core.current import try_get_databand_run
from dbnd._core.errors.base import (
    DatabandApiError,
    DatabandAuthenticationError,
    DatabandConnectionException,
    DatabandUnauthorizedApiError,
)
from dbnd._core.errors.friendly_error.api import (
    api_connection_refused,
    unauthorized_api_call,
)
from dbnd._core.log.logging_utils import create_file_handler
from dbnd._core.utils.http.retry_policy import LinearRetryPolicy
from dbnd._vendor import curlify


# we'd like to have all requests with default timeout, just in case it's stuck
DEFAULT_REQUEST_TIMEOUT = 300

logger = logging.getLogger(__name__)


# uncomment for requests trace
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class ApiClient(object):
    """Json API client implementation."""

    api_prefix = "/api/v1/"
    default_headers = None

    def __init__(
        self,
        api_base_url,  # type: str
        credentials=None,  # type: Optional[Dict[str, str]]
        debug_server=False,  # type: bool
        session_timeout=5,  # type: int
        default_max_retry=1,  # type: int
        default_retry_sleep=0,  # type: Union[int, float]
        default_request_timeout=DEFAULT_REQUEST_TIMEOUT,  # Union[float, Tuple[float, float]],
    ):
        """
        @param api_base_url: databand webserver url to build the request with
        @param credentials: dict of credential to authenticate with the webserver
         can include "token" key or "username"  and "password" keys
        @param debug_server: flag to debug the webserver - collect logs from webserver and the client's requests
        @param session_timeout: minutes to recreate the requests session
        @param default_max_retry: default value for retries for failed connection
        @param default_retry_sleep: default value for sleep between retries
        @param default_request_timeout: (optional) How long to wait for the server to send
            data before giving up, as a float, or a :ref:`(connect timeout,
            read timeout) <timeouts>` tuple
        """

        self._api_base_url = api_base_url
        self.is_auth_required = bool(credentials)
        self.credentials = credentials
        self.default_headers = {"Accept": "application/json"}

        self.session = None
        self.session_creation_time = None
        self.session_timeout = session_timeout

        self.default_max_retry = default_max_retry
        self.default_retry_sleep = default_retry_sleep
        self.default_request_timeout = default_request_timeout

        self.debug_mode = debug_server
        if debug_server:
            # the header require so the webserver will record logs and sql queries
            self.default_headers["X-Databand-Debug"] = "True"
            # log the request curl command here
            self.requests_logger = build_file_logger("requests", fmt="%(message)s")
            # log the webserver logs and sql queries here
            self.webserver_logger = build_file_logger("webserver")

    def remove_session(self):
        self.session = None
        self.session_creation_time = None

    def is_session_timedout(self):
        return datetime.now() - self.session_creation_time >= timedelta(
            minutes=self.session_timeout
        )

    def _request(
        self,
        endpoint,
        method="GET",
        data=None,
        headers=None,
        query=None,
        request_timeout=None,
    ):
        if not self.session or self.is_session_timedout():
            logger.info(
                "Webserver session does not exist or timedout, creating new one"
            )
            self._init_session(self.credentials)

        headers = dict(self.default_headers, **(headers or {}))
        url = urljoin(self._api_base_url, endpoint)
        try:
            request_params = dict(
                method=method,
                url=url,
                json=data,
                headers=headers,
                params=query,
                timeout=request_timeout or self.default_request_timeout,
            )
            logger.debug("Sending the following request: %s", request_params)
            resp = self._send_request(**request_params)

        except requests.exceptions.ConnectionError as ce:
            logger.info("Got connection error while sending request: {}".format(ce))
            self.remove_session()
            raise

        if self.debug_mode:
            # save the curl of the current request
            curl_request = curlify.to_curl(resp.request)
            self.requests_logger.info(curl_request)

        if not resp.ok:
            logger.info("Response is not ok, Raising DatabandApiError")
            if resp.status_code in [403, 401]:
                raise unauthorized_api_call(method, url, resp)

            raise DatabandApiError(
                method, url, resp.status_code, resp.content.decode("utf-8")
            )

        if resp.content:
            try:
                data = resp.json()
            except Exception as e:
                logger.info("Failed to get resp.json(). Exception: {}".format(e))
            else:
                if self.debug_mode and isinstance(data, dict):
                    msg = "api_call: {request}\ndebug info:{debug_data}\n".format(
                        request=resp.request.url, debug_data=data.get("debug")
                    )
                    self.webserver_logger.info(msg)

                return data

        return

    def _send_request(self, method, url, **kwargs):
        return self.session.request(method, url, **kwargs)

    def _init_session(self, credentials):
        logger.info("Initialising session for webserver")
        try:
            self.session = requests.session()
            self.session_creation_time = datetime.now()

            if not self.is_auth_required:
                return

            token = credentials.get("token")
            if token:
                self.default_headers["Authorization"] = "Bearer {}".format(token)
                return

            # get the csrf token cookie (if enabled on the server)
            self._send_request("GET", urljoin(self._api_base_url, "/app"))
            csrf_token = self.session.cookies.get("dbnd_csrftoken")
            if csrf_token:
                logger.info("Got csrf token from session")
                self.default_headers["X-CSRFToken"] = csrf_token

            if "username" in credentials and "password" in credentials:
                logger.info("Attempting to login to webserver")
                self.api_request(
                    "auth/login", method="POST", data=credentials,
                )
            else:
                logger.warning(
                    "ApiClient._init_session: username or password is not provided"
                )
        except requests.exceptions.ConnectionError:
            self.remove_session()
            raise

        except Exception as e:
            self.remove_session()
            raise DatabandAuthenticationError("Failed to init a webserver session", e)

    def api_request(
        self,
        endpoint,
        data,
        method="POST",
        headers=None,
        query=None,
        no_prefix=False,
        retry_policy=None,
        failure_handler=None,
        request_timeout=None,
    ):
        retry_policy = retry_policy or LinearRetryPolicy(
            seconds_to_sleep=self.default_retry_sleep,
            max_retries=self.default_max_retry,
        )
        url = endpoint if no_prefix else urljoin(self.api_prefix, endpoint)

        retry_number = 0
        while True:
            retry_number += 1
            try:
                resp = self._request(
                    url,
                    method=method,
                    data=data,
                    headers=headers,
                    query=query,
                    request_timeout=request_timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as ex:
                if failure_handler:
                    failure_handler(ex, retry_policy, retry_number)

                if retry_policy.should_retry(500, None, retry_number):
                    sleep(retry_policy.seconds_to_sleep(retry_number))
                    continue

                raise api_connection_refused(self._api_base_url + url, ex)
            return resp

    def is_ready(self):
        try:
            is_auth_required = self.is_auth_required
            self.is_auth_required = False
            self.api_request("auth/ping", None, method="GET")
            return True
        except (DatabandConnectionException, DatabandApiError):
            return False
        finally:
            self.is_auth_required = is_auth_required

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, self._api_base_url)


def build_file_logger(name, fmt=None):
    """
    Create a logger which write only to a file.
    the file will be located under the run dict.
    """
    file_logger = logging.getLogger("{}_{}".format(__name__, name))
    file_logger.propagate = False

    run = try_get_databand_run()
    if run:
        log_file = run.run_local_root.partition("{}.logs".format(name))
        logger.info(
            "Api-clients {name} logs writing into {path}".format(
                name=name, path=log_file
            )
        )
        handler = create_file_handler(str(log_file), fmt=fmt)
        file_logger.addHandler(handler)
        file_logger.setLevel(logging.INFO)

    return file_logger
