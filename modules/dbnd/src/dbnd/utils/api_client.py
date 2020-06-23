import logging

import requests
import six

from six.moves.urllib_parse import urljoin

from dbnd._core.errors.base import DatabandApiError, DatabandConnectionException
from dbnd._core.errors.friendly_error.api import api_connection_refused


logger = logging.getLogger(__name__)


# uncomment for requests trace
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class ApiClient(object):
    """Json API client implementation."""

    api_prefix = "/api/v1/"

    def __init__(self, api_base_url, auth=None, user="databand", password="databand"):
        self._api_base_url = api_base_url
        self.auth = auth
        self.user = user
        self.password = password
        self.session = None

    def _request(self, endpoint, method="GET", data=None, headers=None, query=None):
        if headers is None:
            headers = {}
        if not self.session:
            self._init_session()

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

    def _init_session(self):
        try:
            self.session = requests.session()

            # get the csrf token cookie (if enabled on the server)
            self.session.get(urljoin(self._api_base_url, "/app"))
            csrf_token = self.session.cookies.get("dbnd_csrftoken")
            if csrf_token:
                self.session.headers["X-CSRFToken"] = csrf_token

            if self.auth:
                self.api_request(
                    "auth/login",
                    method="POST",
                    data={"username": self.user, "password": self.password},
                )  # TODO ...you know what to do
        except Exception:
            self.session = None
            raise

    def api_request(
        self, endpoint, data, method="POST", headers=None, query=None, no_prefix=False
    ):
        url = endpoint if no_prefix else urljoin(self.api_prefix, endpoint)
        try:
            resp = self._request(
                url, method=method, data=data, headers=headers, query=query
            )
        except requests.ConnectionError as ex:
            raise api_connection_refused(self._api_base_url + url, ex)
        return resp

    def is_ready(self):
        try:
            self.api_request("/", None, method="HEAD", no_prefix=True)
            return True
        except (DatabandConnectionException, DatabandApiError):
            return False


def dict_dump(obj_dict, value_schema):
    """
    Workaround around marshmallow 2.0 not supporting Dict with types
    :return:
    """
    return {
        key: value_schema.dump(value).data for key, value in six.iteritems(obj_dict)
    }
