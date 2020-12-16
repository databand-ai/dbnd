import logging

from time import sleep

import requests
import six

from six.moves.urllib_parse import urljoin

from dbnd._core.errors.base import DatabandApiError, DatabandConnectionException
from dbnd._core.errors.friendly_error.api import api_connection_refused
from dbnd._core.utils.http.retry_policy import LinearRetryPolicy


logger = logging.getLogger(__name__)


# uncomment for requests trace
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class ApiClient(object):
    """Json API client implementation."""

    api_prefix = "/api/v1/"
    default_headers = None

    def __init__(self, api_base_url, credentials=None):
        self._api_base_url = api_base_url
        self.is_auth_required = bool(credentials)
        self.credentials = credentials
        self.session = None
        self.default_headers = {"Accept": "application/json"}

    def _request(self, endpoint, method="GET", data=None, headers=None, query=None):
        if not self.session:
            self._init_session(self.credentials)

        headers = dict(self.default_headers, **(headers or {}))

        url = urljoin(self._api_base_url, endpoint)
        try:
            resp = self.session.request(
                method=method, url=url, json=data, headers=headers, params=query
            )
        except requests.exceptions.ConnectionError:
            self.session = None
            raise

        if not resp.ok:
            raise DatabandApiError(
                method, url, resp.status_code, resp.content.decode("utf-8")
            )
        if resp.content:
            try:
                return resp.json()
            except Exception:
                return None
        return resp.json() if resp.content else None

    def _init_session(self, credentials):
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
                self.default_headers["X-CSRFToken"] = csrf_token

            if "username" in credentials and "password" in credentials:
                self.api_request(
                    "auth/login", method="POST", data=credentials,
                )
            else:
                logger.warning(
                    "ApiClient._init_session: username or password is not provided"
                )
        except Exception:
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


def dict_dump(obj_dict, value_schema):
    """
    Workaround around marshmallow 2.0 not supporting Dict with types
    :return:
    """
    return {
        key: value_schema.dump(value).data for key, value in six.iteritems(obj_dict)
    }
