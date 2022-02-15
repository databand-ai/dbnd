import logging

from typing import Optional

from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.shared.error_handler import capture_monitor_exception
from airflow_monitor.shared.runners.base_runner import BaseRunner
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


class SequentialRunner(BaseRunner):
    def __init__(self, target, **kwargs):
        super(SequentialRunner, self).__init__(target, **kwargs)

        self._iteration = -1
        self._running = None  # type: Optional[BaseMonitorSyncer]

    @capture_monitor_exception
    def start(self):
        if self._running:
            logger.warning("Already running")
            return

        self._iteration = -1
        self._running = self.target(**self.kwargs, run=False)

    @capture_monitor_exception
    def stop(self):
        self._running = None

    @capture_monitor_exception
    def heartbeat(self):
        if self._running:
            # Refresh _running config incaese sleep_interval changed
            self._running.refresh_config()
            if (
                self.last_heartbeat
                and (utcnow() - self.last_heartbeat).total_seconds()
                < self._running.sleep_interval - 1
            ):
                return

            try:
                self._iteration += 1
                logger.info("Running sync for %s", self)
                self._running.sync_once()
            except Exception:
                self._running = None
                raise
            finally:
                self.last_heartbeat = utcnow()

    @capture_monitor_exception
    def is_alive(self):
        return bool(self._running)

    def __str__(self):
        s = super(SequentialRunner, self).__str__()
        return f"{s}({self._running}, iter={self._iteration})"
