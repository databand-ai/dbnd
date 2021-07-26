import json
import logging
from time import sleep

import requests

from dbnd._core.errors import DatabandError, DatabandConfigError
from dbnd._core.utils.http import constants

# copypasted from sparkmagic package
# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
#


logger = logging.getLogger(__name__)


class HttpClientException(DatabandError):
    pass


class ReliableHttpClient(object):
    """Http client that is reliable in its requests. Uses requests library."""

    def __init__(self, endpoint, headers, retry_policy, ignore_ssl_errors=False):
        self._endpoint = endpoint
        self._headers = headers
        self._retry_policy = retry_policy
        if self._endpoint.auth == constants.AUTH_KERBEROS:
            from requests_kerberos import HTTPKerberosAuth, REQUIRED

            self._auth = HTTPKerberosAuth(mutual_authentication=REQUIRED)
        elif self._endpoint.auth == constants.AUTH_BASIC:
            self._auth = (self._endpoint.username, self._endpoint.password)
        elif self._endpoint.auth != constants.NO_AUTH:
            raise DatabandConfigError("Unsupported auth %s" % self._endpoint.auth)

        self.logger = logger

        self.verify_ssl = not ignore_ssl_errors
        if not self.verify_ssl:
            self.logger.debug(
                "ATTENTION: Will ignore SSL errors. This might render you vulnerable to attacks."
            )
            requests.packages.urllib3.disable_warnings()

    def get_headers(self):
        return self._headers

    def compose_url(self, relative_url):
        r_u = "/{}".format(relative_url.rstrip("/").lstrip("/"))
        return self._endpoint.url + r_u

    def get(self, relative_url, accepted_status_codes):
        """Sends a get request. Returns a response."""
        logger.debug("Sending GET request to %s", self.compose_url(relative_url))
        return self._send_request(relative_url, accepted_status_codes, requests.get)

    def post(self, relative_url, accepted_status_codes, data):
        """Sends a post request. Returns a response."""
        logger.debug(
            "Sending POST request to %s, with data: %s",
            self.compose_url(relative_url),
            data,
        )
        return self._send_request(
            relative_url, accepted_status_codes, requests.post, data
        )

    def delete(self, relative_url, accepted_status_codes):
        """Sends a delete request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, requests.delete)

    def _send_request(self, relative_url, accepted_status_codes, function, data=None):
        response = self._send_request_helper(
            self.compose_url(relative_url), accepted_status_codes, function, data, 0
        )
        logger.debug("Received response: %s", response)
        return response

    def _send_request_helper(
        self, url, accepted_status_codes, function, data, retry_count
    ):
        while True:
            try:
                if self._endpoint.auth == constants.NO_AUTH:
                    if data is None:
                        r = function(url, headers=self._headers, verify=self.verify_ssl)
                    else:
                        r = function(
                            url,
                            headers=self._headers,
                            data=json.dumps(data),
                            verify=self.verify_ssl,
                        )
                else:
                    if data is None:
                        r = function(
                            url,
                            headers=self._headers,
                            auth=self._auth,
                            verify=self.verify_ssl,
                        )
                    else:
                        r = function(
                            url,
                            headers=self._headers,
                            auth=self._auth,
                            data=json.dumps(data),
                            verify=self.verify_ssl,
                        )
            except requests.exceptions.RequestException as e:
                error = True
                r = None
                status = None
                text = None

                self.logger.warning("Request to '{}' failed with '{}'".format(url, e))
            else:
                error = False
                status = r.status_code
                text = r.text

            if error or status not in accepted_status_codes:
                if self._retry_policy.should_retry(status, error, retry_count):
                    sleep(self._retry_policy.seconds_to_sleep(retry_count))
                    retry_count += 1
                    continue

                if error:
                    raise HttpClientException(
                        "Error sending http request and maximum retry encountered."
                    )
                else:
                    raise HttpClientException(
                        "Invalid status code '{}' from {} with error payload: {}".format(
                            status, url, text
                        )
                    )
            return r
