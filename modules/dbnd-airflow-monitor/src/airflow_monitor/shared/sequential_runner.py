# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import datetime
from typing import Optional

from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from airflow_monitor.shared.error_handler import capture_monitor_exception
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


class SequentialRunner:
    def __init__(self, target, tracking_service, **kwargs):
        self.target: BaseMonitorSyncer = target
        self.tracking_service: BaseDbndTrackingService = tracking_service
        self.kwargs: dict = kwargs

        self.last_heartbeat: Optional[datetime] = None

        self._iteration = -1

    @capture_monitor_exception
    def heartbeat(self, is_last=False):
        # Refresh _running config incaese sleep_interval changed
        self.target.refresh_config()
        if self._should_sync(is_last):
            try:
                self._iteration += 1
                self.target.sync_once()
            finally:
                self.last_heartbeat = utcnow()

    def _sleep_interval_not_met(self):
        return (
            self.last_heartbeat
            and (utcnow() - self.last_heartbeat).total_seconds()
            < self.target.sleep_interval - 1
        )

    def __str__(self):
        s1 = ", ".join([f"{k}={v}" for k, v in self.kwargs.items()])
        s2 = f"{self.__class__.__name__}({self.target.__class__.__name__}, {s1})"
        return f"{s2}({self.target}, iter={self._iteration})"

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

    def tracking_source_uid(self):
        if not self.kwargs:
            return None
        return self.kwargs.get("tracking_source_uid")
