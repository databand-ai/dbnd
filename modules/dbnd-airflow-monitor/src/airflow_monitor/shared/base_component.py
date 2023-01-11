# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from dbnd.utils.trace import new_tracing_id


class BaseComponent:
    """
    BaseComponent is a component responsible for syncing data from given server to tracking service
    """

    config: BaseServerConfig
    tracking_service: BaseTrackingService
    syncer_management_service: BaseSyncerManagementService
    data_fetcher: object
    sleep_interval: int

    def __init__(
        self,
        config: BaseServerConfig,
        tracking_service: BaseTrackingService,
        syncer_management_service,
        data_fetcher: object,
    ):
        self.config = config
        self.tracking_service = tracking_service
        self.syncer_management_service = syncer_management_service
        self.data_fetcher: object = data_fetcher
        self.last_heartbeat = None

    @property
    def sleep_interval(self):
        return self.config.sync_interval

    def refresh_config(self, config):
        self.config = config
        if (
            self.config.log_level
            and logging.getLevelName(self.config.log_level) != logging.root.level
        ):
            logging.root.setLevel(self.config.log_level)

    def sync_once(self):
        from airflow_monitor.shared.error_handler import capture_component_exception

        with new_tracing_id(), capture_component_exception(self, "sync_once"):
            return self._sync_once()

    def _sync_once(self):
        raise NotImplementedError()

    def stop(self):
        pass

    def __str__(self):
        return f"{self.__class__.__name__}({self.config.source_name}|{self.config.identifier})"
