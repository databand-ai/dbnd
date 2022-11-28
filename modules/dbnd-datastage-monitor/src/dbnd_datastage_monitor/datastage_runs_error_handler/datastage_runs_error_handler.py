# © Copyright Databand.ai, an IBM Company 2022

import logging
import queue

from abc import ABC
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

from dbnd._vendor.cachetools import TTLCache


logger = logging.getLogger(__name__)


class DataStageFailedRun:
    def __init__(self, run_link: str, retry_attempt: int):
        self.run_link = run_link
        self.run_id = self._parse_run_id()
        self.project_id = self._parse_project_id()
        self.retry_attempt = retry_attempt

    def _parse_run_id(self):
        return urlparse(self.run_link).path.split("/")[3]

    def _parse_project_id(self):
        return urlparse(self.run_link).query.split("=")[1]


class DatastageRunsErrorHandler(ABC):
    def submit_failed_run(self, run_link: str):
        raise NotImplementedError

    def submit_failed_runs(self, run_links: List[str]):
        raise NotImplementedError

    def pull_failed_runs(self, batch_size: int):
        raise NotImplementedError


class DatastageRunsErrorQueue(DatastageRunsErrorHandler):
    MAX_RETRIES = 3
    MAX_CACHE_SIZE = 1000
    CACHE_TTL_HOURS = 12
    MAX_QUEUE_SIZE = 1000

    def __init__(self, max_retries=MAX_RETRIES):
        # thread safe queue
        self.failed_runs_queue = queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self.failed_runs_retries_cache = TTLCache(
            maxsize=self.MAX_CACHE_SIZE,
            ttl=timedelta(hours=self.CACHE_TTL_HOURS),
            timer=datetime.now,
        )
        self.max_retries = max_retries

    def submit_failed_runs(self, run_links: List[str]):
        for run_link in run_links:
            logger.debug("submitting failed run %s to error handler", run_link)
            self.submit_failed_run(run_link)

    def submit_failed_run(self, run_link: str):
        try:
            logger.debug("submit run %s to fail runs retry queue", run_link)
            failed_run = DataStageFailedRun(run_link=run_link, retry_attempt=0)
            self.failed_runs_queue.put(item=failed_run)
        except Exception:
            logger.exception("failed to submit failed run for retry")

    def pull_failed_runs(self, batch_size: int):
        failed_runs_to_retry = []
        for i in range(batch_size):
            try:
                failed_run_to_retry = self.failed_runs_queue.get(block=False)
                run_link = failed_run_to_retry.run_link
                retries = failed_run_to_retry.retry_attempt
                if run_link in self.failed_runs_retries_cache:
                    retries = self.failed_runs_retries_cache.get(key=run_link)
                if retries >= self.max_retries:
                    # run can be deleted from cache since it will not be retried
                    self.failed_runs_retries_cache.pop(key=run_link)
                    logger.debug(
                        "run %s retry attempt reached max retry of , run will not be retried",
                        run_link,
                    )
                    continue
                retries += 1
                self.failed_runs_retries_cache[run_link] = retries
                failed_run_to_retry.retry_attempt = retries
            except queue.Empty:
                logger.debug("failed runs queue is empty")
                return failed_runs_to_retry
            logger.debug(
                "pull a failed run %s for project %s retry attempt %s from queue",
                failed_run_to_retry.run_link,
                failed_run_to_retry.project_id,
                failed_run_to_retry.retry_attempt,
            )
            failed_runs_to_retry.append(failed_run_to_retry)
        return failed_runs_to_retry
