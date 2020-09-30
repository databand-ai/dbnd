# Partially copied from sparkmagic package
# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json
import logging
import time

from dbnd._core.errors import DatabandError
from dbnd._core.utils.http.reliable_http_client import ReliableHttpClient
from dbnd._core.utils.http.retry_policy import get_retry_policy


logger = logging.getLogger(__name__)

BATCH_RUNNING_STATES = ["starting", "not_started", "running", "recovering"]
BATCH_ERROR_STATES = ["error", "dead"]


class LivyBatchClient(object):
    """A Livy-specific Http client which wraps the normal ReliableHttpClient. Propagates
    HttpClientExceptions up."""

    def __init__(self, http_client, endpoint):
        self.endpoint = endpoint
        self._http_client = http_client

    @staticmethod
    def from_endpoint(endpoint, ignore_ssl_errors=False):
        headers = {"Content-Type": "application/json"}
        custom_headers = {}

        headers.update(custom_headers)
        retry_policy = LivyBatchClient._get_retry_policy()
        return LivyBatchClient(
            ReliableHttpClient(
                endpoint, headers, retry_policy, ignore_ssl_errors=ignore_ssl_errors
            ),
            endpoint,
        )

    def post_batch(self, properties):
        return self._http_client.post("/batches", [201], properties).json()

    def get_batch(self, batch_id):
        return self._http_client.get(self._batch_url(batch_id), [200]).json()

    def get_batch_state(self, batch_id):
        return self._http_client.get(self._batch_url(batch_id), [200]).json()

    def get_all_batch_logs(self, batch_id, from_line=0):
        return self._http_client.get(
            self._batch_url(batch_id) + "/log?from=%s" % from_line, [200]
        ).json()

    def get_batches(self):
        return self._http_client.get("/batches", [200]).json()

    def delete_batch(self, batch_id):
        self._http_client.delete(self._batch_url(batch_id), [200, 404])

    def get_headers(self):
        return self._http_client.get_headers()

    @staticmethod
    def _batch_url(batch_id):
        return "/batches/{}".format(batch_id)

    @staticmethod
    def _get_retry_policy():
        return get_retry_policy("LivyBatch'")

        # Function to help track the progress of the scala code submitted to Apache Livy

    def _print_status(self, response):
        logger.info

    def _default_status_reporter(self, batch_response):
        logger.info(
            "Batch status: %s", json.dumps(batch_response, indent=4, sort_keys=True)
        )

    def track_batch_progress(self, batch_id, status_reporter=None):
        status_reporter = status_reporter or self._default_status_reporter

        # Poll the status of the submitted scala code

        current_line = 0
        while True:
            batch_response = self.get_batch(batch_id)
            batch_status = batch_response["state"]
            status_reporter(batch_response)

            # logging the logs
            lines = self.get_all_batch_logs(batch_id, from_line=current_line)["log"]
            for line in lines:
                logger.info(line)
            current_line += len(lines)
            if batch_status.lower() not in BATCH_RUNNING_STATES:
                break
            time.sleep(10)

        status_reporter(batch_response)
        if batch_status.lower() in BATCH_ERROR_STATES:
            logger.info("Batch exception: see logs")
            raise DatabandError("Batch Status: " + batch_status)
        logger.info("Batch Status: " + batch_status)
