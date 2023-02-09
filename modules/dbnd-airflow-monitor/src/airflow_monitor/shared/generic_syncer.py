# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Iterable

from airflow_monitor.shared.adapter.adapter import Adapter
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)


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
        integration_management_service: IntegrationManagementService,
        adapter: Adapter,
    ):
        super(GenericSyncer, self).__init__(
            config, tracking_service, integration_management_service, None
        )
        self.adapter = adapter

    def _sync_once(self):
        logger.info(
            "Started running for tracking source %s", self.config.tracking_source_uid
        )

        cursor = self.tracking_service.get_last_cursor(
            integration_id=self.config.identifier
        )

        if cursor is None:
            self.tracking_service.update_last_cursor(
                integration_id=self.config.identifier,
                state="init",
                data=self.adapter.get_last_cursor(),
            )
            return

        synced_updated_data = self.sync_active_data()
        synced_new_data = self.sync_new_data(cursor)

        # report last cursor only when all pages saved
        self.tracking_service.update_last_cursor(
            integration_id=self.config.identifier,
            state="update",
            data=self.adapter.get_last_cursor(),
        )
        self.integration_management_service.report_monitor_time_data(
            self.config.identifier,
            synced_new_data=(synced_updated_data or synced_new_data),
        )

    def sync_active_data(self) -> bool:
        update_data = self.update_active_adapter_data()
        if update_data:
            self.tracking_service.save_tracking_data(update_data)
            return True

        logger.info(
            "No updated data found for tracking source %s",
            self.config.tracking_source_uid,
        )
        return False

    def update_active_adapter_data(self) -> dict:
        logger.info(
            "Checking for in progress data for tracking source %s",
            self.config.tracking_source_uid,
        )
        active_runs = self.tracking_service.get_active_runs()
        if active_runs:
            return self.adapter.get_update_data(active_runs)
        return {}

    def sync_new_data(self, cursor: object) -> bool:
        synced_new_runs = False
        logger.info(
            "Checking for new data for tracking source %s",
            self.config.tracking_source_uid,
        )
        for data in self.get_adapter_data(cursor):
            if data:
                self.tracking_service.save_tracking_data(data)
                synced_new_runs = True

        if not synced_new_runs:
            logger.info(
                "No new data found for tracking source %s",
                self.config.tracking_source_uid,
            )

        return synced_new_runs

    def get_adapter_data(self, cursor: object) -> Iterable[object]:
        next_page = None
        while True:
            adapter_data = self.adapter.get_new_data(
                cursor=cursor, batch_size=200, next_page=next_page
            )
            data = adapter_data.data
            next_page = adapter_data.next_page
            # TODO: put failed in queue here
            if next_page is None:
                break
            yield data
        yield data
