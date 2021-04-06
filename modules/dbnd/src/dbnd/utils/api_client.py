import logging

from time import sleep

import requests
import six

from six.moves.urllib_parse import urljoin

from dbnd._core.current import try_get_current_task_run, try_get_databand_run
from dbnd._core.errors.base import DatabandApiError, DatabandConnectionException
from dbnd._core.errors.friendly_error.api import api_connection_refused
from dbnd._core.log.logging_utils import create_file_handler
from dbnd._core.utils.http.retry_policy import LinearRetryPolicy
from dbnd._vendor import curlify


logger = logging.getLogger(__name__)


# uncomment for requests trace
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class ApiClient(object):
    """Json API client implementation."""

    api_prefix = "/api/v1/"
    default_headers = None

    def __init__(self, api_base_url, credentials=None, debug_server=False):
        self._api_base_url = api_base_url
        self.is_auth_required = bool(credentials)
        self.credentials = credentials
        self.session = None
        self.default_headers = {"Accept": "application/json"}
        self.debug_mode = debug_server
        if debug_server:
            # the header require so the webserver will record logs and sql queries
            self.default_headers["X-Databand-Debug"] = "True"
            # log the request curl command here
            self.requests_logger = build_file_logger("requests", fmt="%(message)s")
            # log the webserver logs and sql queries here
            self.webserver_logger = build_file_logger("webserver")

    def _request(self, endpoint, method="GET", data=None, headers=None, query=None):
        if not self.session:
            logger.info("Webserver session does not exist, creating new one")
            self._init_session(self.credentials)

        headers = dict(self.default_headers, **(headers or {}))

        url = urljoin(self._api_base_url, endpoint)
        try:
            request_params = dict(
                method=method, url=url, json=data, headers=headers, params=query
            )
            logger.debug("Sending the following request: %s", request_params)
            resp = self.session.request(**request_params)
        except requests.exceptions.ConnectionError as ce:
            logger.info("Got connection error while sending request: {}".format(ce))
            self.session = None
            raise

        if self.debug_mode:
            # save the curl of the current request
            curl_request = curlify.to_curl(resp.request)
            self.requests_logger.info(curl_request)

        if not resp.ok:
            logger.info("Response is not ok, Raising DatabandApiError")
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

    def _init_session(self, credentials):
        logger.info("Initialising session for webserver")
        try:
            self.session = requests.session()

            if not self.is_auth_required:
                return

            token = credentials.get("token")
            if token:
                self.default_headers["Authorization"] = "Bearer {}".format(token)
                return

            # get the csrf token cookie (if enabled on the server)
            self.session.get(urljoin(self._api_base_url, "/app"))
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
        except Exception as e:
            logger.warning(
                "Exception occurred while initialising the session: {}".format(e)
            )
            self.session = None
            raise

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
    ):
        retry_number = 0
        retry_policy = retry_policy or LinearRetryPolicy(0, 1)  # single chance
        url = endpoint if no_prefix else urljoin(self.api_prefix, endpoint)
        while True:
            retry_number += 1
            try:
                resp = self._request(
                    url, method=method, data=data, headers=headers, query=query
                )
            except requests.ConnectionError as ex:
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


def dict_dump(obj_dict, value_schema):
    """
    Workaround around marshmallow 2.0 not supporting Dict with types
    :return:
    """
    return {
        key: value_schema.dump(value).data for key, value in six.iteritems(obj_dict)
    }


def build_file_logger(name, fmt=None):
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
