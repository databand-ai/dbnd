import logging
import time

from typing import Type

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher import AirflowDataFetcher, get_data_fetcher
from airflow_monitor.tracking_service import (
    DbndAirflowTrackingService,
    get_tracking_service,
)


class BaseMonitorComponent(object):
    config: AirflowServerConfig
    data_fetcher: AirflowDataFetcher
    tracking_service: DbndAirflowTrackingService

    def __init__(
        self,
        config: AirflowServerConfig,
        data_fetcher: AirflowDataFetcher,
        tracking_service: DbndAirflowTrackingService,
    ):
        self.config = config
        self.data_fetcher = data_fetcher
        self.tracking_service = tracking_service
        self.sleep_interval = config.interval

    def run(self):
        while True:
            self.sync_once()
            time.sleep(self.sleep_interval)

    @capture_monitor_exception
    def refresh_config(self):
        self.config = self.tracking_service.get_airflow_server_configuration()
        if (
            self.config.log_level
            and logging.getLevelName(self.config.log_level) != logging.root.level
        ):
            logging.root.setLevel(self.config.log_level)

    def sync_once(self):
        self.refresh_config()
        return self._sync_once()

    def _sync_once(self):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.__class__.__name__}({self.config.name}|{self.config.tracking_source_uid})"


def start_syncer(factory: Type[BaseMonitorComponent], tracking_source_uid, run=True):
    tracking_service = get_tracking_service(tracking_source_uid=tracking_source_uid)
    monitor_config = tracking_service.get_airflow_server_configuration()
    data_fetcher = get_data_fetcher(monitor_config)
    syncer = factory(monitor_config, data_fetcher, tracking_service)
    if run:
        syncer.run()
    return syncer
