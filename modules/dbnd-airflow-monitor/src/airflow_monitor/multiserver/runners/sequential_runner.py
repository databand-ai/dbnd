import logging

from typing import Optional

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.base_component import BaseMonitorComponent
from airflow_monitor.multiserver.runners.base_runner import BaseRunner


logger = logging.getLogger(__name__)


class SequentialRunner(BaseRunner):
    def __init__(self, target, **kwargs):
        super(SequentialRunner, self).__init__(target, **kwargs)

        self._iteration = -1
        self._running = None  # type: Optional[BaseMonitorComponent]

    @capture_monitor_exception(logger)
    def start(self):
        if self._running:
            logger.warning("Already running")
            return

        try:
            self._iteration = -1
            self._running = self.target(**self.kwargs, run=False)
        except Exception as e:
            logger.exception(
                f"Failed to create component: {self.target}({self.kwargs})"
            )

    @capture_monitor_exception(logger)
    def stop(self):
        self._running = None

    @capture_monitor_exception(logger)
    def heartbeat(self):
        if self._running:
            self._iteration += 1
            try:
                self._running.sync_once()
            except Exception as e:
                logger.exception(
                    f"Failed to run sync iteration {self._iteration}: {self.target}({self.kwargs}"
                )
                self._running = None

    @capture_monitor_exception(logger)
    def is_alive(self):
        return bool(self._running)

    def __str__(self):
        s = super(SequentialRunner, self).__str__()
        return f"{s}({self._running}, iter={self._iteration})"
