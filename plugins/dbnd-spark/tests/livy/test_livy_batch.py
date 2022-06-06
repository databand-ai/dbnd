import unittest

from unittest import mock

import pytest

from requests import Response

from dbnd._core.utils.http.constants import AUTH_BASIC
from dbnd._core.utils.http.endpoint import Endpoint
from dbnd._core.utils.http.reliable_http_client import HttpClientException
from dbnd_spark.livy.livy_batch import LivyBatchClient


def mocked_requests_get_with_401(*args, **kwargs):
    response = Response()
    response.status_code = 401
    return response


class TestLivyBatchRetry(unittest.TestCase):
    @mock.patch("requests.get", side_effect=mocked_requests_get_with_401)
    def test_get_batch_retries_on_invalid_status_code(self, requests_get_mock):
        max_retries = 5
        x = LivyBatchClient.from_endpoint(
            self.fake_endpoint(), max_retries, status_code_retries_delay=0
        )

        with pytest.raises(HttpClientException):
            x.get_batch("non-existing-batch")

        assert 1 + max_retries == requests_get_mock.call_count

    def fake_endpoint(self):
        return Endpoint("doesnt-exist", auth=AUTH_BASIC, username="u", password="p")
