# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.adapter.adapter import Adapter
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)
from airflow_monitor.shared.base_tracking_service import BaseTrackingService


logger = logging.getLogger(__name__)


class GenericSyncer(BaseComponent):
    SYNCER_TYPE = "generic_syncer"

    tracking_service: BaseTrackingService
    config: BaseServerConfig
    adapter: Adapter

    def __init__(
        self,
        config: BaseServerConfig,
        tracking_service: BaseTrackingService,
        syncer_management_service: BaseSyncerManagementService,
        adapter: Adapter,
    ):
        super(GenericSyncer, self).__init__(
            config, tracking_service, syncer_management_service, None
        )
        self.adapter = adapter

    def refresh_config(self, config):
        super(GenericSyncer, self).refresh_config(config)

    def _sync_once(self):
        logger.info(
            "Started running for tracking source %s", self.config.tracking_source_uid
        )

        cursor = self.tracking_service.get_last_seen_date()

        if cursor is None:
            self.tracking_service.update_last_seen_values(
                self.adapter.get_last_cursor()
            )
            return

        logger.info(
            "Checking for new data for tracking source %s",
            self.config.tracking_source_uid,
        )
        for chunk in self.get_adapter_data(cursor):
            if chunk:
                # report data in chunks
                self.tracking_service.save_tracking_data(chunk)
            else:
                logger.info(
                    "No new data found for tracking source %s",
                    self.config.tracking_source_uid,
                )
        # report last cursor only when all pages saved
        self.tracking_service.update_last_seen_values(self.adapter.get_last_cursor())
        self.syncer_management_service.update_last_sync_time(self.config.identifier)

    def get_adapter_data(self, cursor):
        next_page = None
        while True:
            (data, failed, next_page) = self.adapter.get_data(
                cursor=cursor, batch_size=200, next_page=next_page
            )
            # TODO: put failed in queue here
            if next_page == None:
                break
            yield data
        yield data
