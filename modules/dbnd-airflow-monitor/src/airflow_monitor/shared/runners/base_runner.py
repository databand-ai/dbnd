# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime
from typing import Optional

from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService


class BaseRunner(object):
    def __init__(self, target, tracking_service, **kwargs):
        self.target: BaseMonitorSyncer = target
        self.tracking_service: BaseDbndTrackingService = tracking_service
        self.kwargs: dict = kwargs

        self.last_heartbeat: Optional[datetime] = None

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def heartbeat(self, is_last=False):
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()

    def __str__(self):
        s = ", ".join([f"{k}={v}" for k, v in self.kwargs.items()])
        return f"{self.__class__.__name__}({self.target.__class__.__name__}, {s})"

    def tracking_source_uid(self):
        if not self.kwargs:
            return None
        return self.kwargs.get("tracking_source_uid")
