# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetsToStatesMachine,
)
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
    assets_to_states_machine: AssetsToStatesMachine
    syncer_instance_id: str

    def __init__(
        self,
        config: BaseServerConfig,
        tracking_service: BaseTrackingService,
        integration_management_service: IntegrationManagementService,
        adapter: Adapter,
        syncer_instance_id: str,
    ):
        super(GenericSyncer, self).__init__(
            config, tracking_service, integration_management_service, None
        )
        self.adapter = adapter
        self.assets_to_states_machine = AssetsToStatesMachine(config.syncer_max_retries)
        self.syncer_instance_id = syncer_instance_id

    def _sync_once(self):
        logger.info(
            "Started running for tracking source %s", self.config.tracking_source_uid
        )
        synced_active_data = False
        synced_new_data = False
        cursor = self.tracking_service.get_last_cursor(
            integration_id=self.config.identifier,
            syncer_instance_id=self.syncer_instance_id,
        )

        if cursor is None:
            self.tracking_service.update_last_cursor(
                integration_id=self.config.identifier,
                syncer_instance_id=self.syncer_instance_id,
                state="init",
                data=self.adapter.init_cursor(),
            )
            return
        logger.info(
            "Checking for new data for tracking source %s",
            self.config.tracking_source_uid,
        )
        active_assets_to_states = self.tracking_service.get_active_assets(
            integration_id=self.config.identifier,
            syncer_instance_id=self.syncer_instance_id,
        )
        if active_assets_to_states:
            synced_active_data = self._process_assets_batch(
                Assets(assets_to_state=active_assets_to_states)
            )

        for init_assets, last_cursor in self.adapter.init_assets_for_cursor(
            cursor, batch_size=10
        ):
            synced_new_data = self._process_assets_batch(init_assets)

        if last_cursor != None:
            # report last cursor only when all pages saved
            self.tracking_service.update_last_cursor(
                integration_id=self.config.identifier,
                syncer_instance_id=self.syncer_instance_id,
                state="update",
                data=last_cursor,
            )
        self.integration_management_service.report_monitor_time_data(
            self.config.identifier,
            synced_new_data=(synced_active_data or synced_new_data),
        )

    def _process_assets_batch(self, assets):
        synced_data = False
        assets_data = self.adapter.get_assets_data(assets)
        assets_data_to_report = assets_data.data
        assets_states_to_report = assets_data.assets_to_state
        if not assets_data_to_report:
            logger.info(
                "No new assets data found for tracking source %s",
                self.config.tracking_source_uid,
            )
        else:
            logger.info(
                "Found new assets data for tracking source %s",
                self.config.tracking_source_uid,
            )
            synced_data = True

            self.tracking_service.save_tracking_data(assets_data_to_report)
        if not assets_states_to_report:
            logger.info(
                "No new assets states for tracking source %s",
                self.config.tracking_source_uid,
            )
        else:
            logger.info(
                "Found new assets states for tracking source %s",
                self.config.tracking_source_uid,
            )
            self.tracking_service.save_assets_state(
                integration_id=self.config.identifier,
                syncer_instance_id=self.syncer_instance_id,
                assets_to_state=self.assets_to_states_machine.process(
                    assets_states_to_report
                ),
            )
        return synced_data

    @property
    def identifier(self) -> str:
        return self.SYNCER_TYPE + "_" + self.syncer_instance_id
