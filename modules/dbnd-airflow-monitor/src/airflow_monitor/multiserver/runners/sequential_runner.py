import logging

from airflow_monitor.multiserver.runners.base_runner import BaseRunner


logger = logging.getLogger(__name__)


class SequentialRunner(BaseRunner):
    def __init__(self, target, **kwargs):
        super(SequentialRunner, self).__init__(target, **kwargs)

        self._iteration = -1
        self._running = None  # type: Optional[BaseAirflowSyncer]

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

    def stop(self):
        self._running = None

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

    def is_alive(self):
        return bool(self._running)
