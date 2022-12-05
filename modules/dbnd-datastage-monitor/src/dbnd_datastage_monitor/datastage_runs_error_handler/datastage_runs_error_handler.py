# © Copyright Databand.ai, an IBM Company 2022

import logging
import queue

from abc import ABC
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

from dbnd_datastage_monitor.metrics.prometheus_metrics import (
    report_completely_run_request_retry_failed,
)

from dbnd._vendor.cachetools import TTLCache


logger = logging.getLogger(__name__)


class DataStageRunRequestRetry:
    def __init__(self, run_link: str, retry_attempt: int):
        self.run_link = run_link
        self.run_id = self._parse_run_id()
        self.project_id = self._parse_project_id()
        self.retry_attempt = retry_attempt

    def _parse_run_id(self):
        return urlparse(self.run_link).path.split("/")[3]

    def _parse_project_id(self):
        return urlparse(self.run_link).query.split("=")[1]


class DatastageRunRequestsRetryHandler(ABC):
    def submit_run_request_retry(self, run_link: str):
        raise NotImplementedError

    def submit_run_request_retries(self, run_links: List[str]):
        raise NotImplementedError

    def pull_run_request_retries(self, batch_size: int):
        raise NotImplementedError


class DatastageRunRequestsRetryQueue(DatastageRunRequestsRetryHandler):
    MAX_RETRIES = 3
    MAX_CACHE_SIZE = 1000
    CACHE_TTL_HOURS = 12
    MAX_QUEUE_SIZE = 1000

    def __init__(self, tracking_source_uid, max_retries=MAX_RETRIES):
        # thread safe queue
        self.run_requests_retry_queue = queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self.run_requests_retries_cache = TTLCache(
            maxsize=self.MAX_CACHE_SIZE,
            ttl=timedelta(hours=self.CACHE_TTL_HOURS),
            timer=datetime.now,
        )
        self.max_retries = max_retries
        self.tracking_source_uid = tracking_source_uid

    def submit_run_request_retries(self, run_links: List[str]):
        for run_link in run_links:
            logger.warning("submitting run request %s to retry handler", run_link)
            self.submit_run_request_retry(run_link)

    def submit_run_request_retry(self, run_link: str):
        try:
            logger.debug("submit run %s to fail runs retry queue", run_link)
            run_retry = DataStageRunRequestRetry(run_link=run_link, retry_attempt=0)
            self.run_requests_retry_queue.put(item=run_retry)
        except Exception:
            logger.exception("failed to submit run request for retry")

    def pull_run_request_retries(
        self, batch_size: int
    ) -> List[DataStageRunRequestRetry]:
        run_requests_to_retry = []
        for i in range(batch_size):
            try:
                run_to_retry = self.run_requests_retry_queue.get(block=False)
                run_link = run_to_retry.run_link
                retries = run_to_retry.retry_attempt
                if run_link in self.run_requests_retries_cache:
                    retries = self.run_requests_retries_cache.get(key=run_link)
                if retries >= self.max_retries:
                    # run can be deleted from cache since it will not be retried
                    self.run_requests_retries_cache.pop(key=run_link)
                    report_completely_run_request_retry_failed(
                        self.tracking_source_uid, run_to_retry.project_id
                    )
                    logger.warning(
                        "run %s retry attempt reached max retry of %s run will not be retried",
                        run_link,
                        self.max_retries,
                    )
                    continue
                retries += 1
                self.run_requests_retries_cache[run_link] = retries
                run_to_retry.retry_attempt = retries
            except queue.Empty:
                logger.debug("run request retries queue is empty")
                return run_requests_to_retry
            logger.warning(
                "pull a run request retry %s for project %s retry attempt %s from queue",
                run_to_retry.run_link,
                run_to_retry.project_id,
                run_to_retry.retry_attempt,
            )
            run_requests_to_retry.append(run_to_retry)
        return run_requests_to_retry

    def get_run_request_retries_queue_size(self) -> int:
        return len(self.run_requests_retry_queue.queue)

    def get_run_request_retries_cache_size(self) -> int:
        return self.run_requests_retries_cache.currsize

    def is_run_retry(self, run_link):
        return run_link in self.run_requests_retries_cache
