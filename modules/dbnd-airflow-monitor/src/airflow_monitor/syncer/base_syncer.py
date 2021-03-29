import time

from typing import Type

from airflow_monitor.common import MonitorConfig
from airflow_monitor.data_fetcher import AirflowDataFetcher, get_data_fetcher
from airflow_monitor.tracking_service import (
    DbndAirflowTrackingService,
    get_tracking_service,
)


class BaseAirflowSyncer(object):
    config: MonitorConfig
    data_fetcher: AirflowDataFetcher
    tracking_service: DbndAirflowTrackingService

    def __init__(
        self,
        config: MonitorConfig,
        data_fetcher: AirflowDataFetcher,
        tracking_service: DbndAirflowTrackingService,
    ):
        self.config = config
        self.data_fetcher = data_fetcher
        self.tracking_service = tracking_service

    def run(self):
        while True:
            self.sync_once()
            time.sleep(self.config.interval)

    def sync_once(self):
        raise NotImplementedError()


def start_syncer(factory: Type[BaseAirflowSyncer], tracking_source_uid, run=True):
    tracking_service = get_tracking_service(tracking_source_uid=tracking_source_uid,)
    monitor_config = tracking_service.get_monitor_configuration()
    data_fetcher = get_data_fetcher(monitor_config)
    syncer = factory(monitor_config, data_fetcher, tracking_service)
    if run:
        syncer.run()
    return syncer
