# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.error_handler import capture_monitor_exception
from airflow_monitor.shared.runners.base_runner import BaseRunner
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


class SequentialRunner(BaseRunner):
    def __init__(self, target, **kwargs):
        super(SequentialRunner, self).__init__(target, **kwargs)

        self._iteration = -1
        self._is_running = False

    @capture_monitor_exception
    def start(self):
        if self._is_running:
            logger.warning("Already running")
            return

        self._iteration = -1
        self._is_running = True

    @capture_monitor_exception
    def stop(self):
        self._is_running = False

    @capture_monitor_exception
    def heartbeat(self, is_last=False):
        if self._is_running:
            # Refresh _running config incaese sleep_interval changed
            self.target.refresh_config()
            if self._should_sync(is_last):
                try:
                    self._iteration += 1
                    self.target.sync_once()
                except Exception:
                    self._is_running = False
                    raise
                finally:
                    self.last_heartbeat = utcnow()

    def _sleep_interval_not_met(self):
        return (
            self.last_heartbeat
            and (utcnow() - self.last_heartbeat).total_seconds()
            < self.target.sleep_interval - 1
        )

    @capture_monitor_exception
    def is_alive(self):
        return self._is_running

    def __str__(self):
        s = super(SequentialRunner, self).__str__()
        return f"{s}({self.target}, iter={self._iteration})"

    def _should_sync(self, is_last):
        if is_last and self.target.sync_last_heartbeat:
            logger.info("Syncing last heartbeat for %s", self)
            return True
        elif is_last and not self.target.sync_last_heartbeat:
            logger.info("Shutting down %s", self)
            return False
        elif self._sleep_interval_not_met():
            logger.debug("Sync interval not met for %s", self)
            return False
        else:
            logger.info("Running sync for %s", self)
            return True
