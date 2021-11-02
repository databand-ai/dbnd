import logging

from time import sleep
from typing import Callable

from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from airflow_monitor.shared.error_handler import capture_monitor_exception
from dbnd.utils.trace import new_tracing_id


class BaseMonitorSyncer:
    """
    BaseMonitorSyncer is componet responsible for syncing data from given server to tracking service
    """

    config: BaseServerConfig
    tracking_service: BaseDbndTrackingService
    data_fetcher: Callable
    sleep_interval: int

    def __init__(
        self,
        config: BaseServerConfig,
        tracking_service: BaseDbndTrackingService,
        data_fetcher: Callable,
    ):
        self.config = config
        self.tracking_service = tracking_service
        self.data_fetcher: Callable = data_fetcher

    @property
    def sleep_interval(self):
        return self.config.sync_interval

    def run(self):
        while True:
            self.sync_once()
            sleep(self.sleep_interval)

    @capture_monitor_exception
    def refresh_config(self):
        self.config = self.tracking_service.get_monitor_configuration()
        # TODO: Raise warning on major config change
        if (
            self.config.log_level
            and logging.getLevelName(self.config.log_level) != logging.root.level
        ):
            logging.root.setLevel(self.config.log_level)

    def sync_once(self):
        with new_tracing_id():
            self.refresh_config()
            return self._sync_once()

    def _sync_once(self):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.__class__.__name__}({self.config.source_name}|{self.config.tracking_source_uid})"
