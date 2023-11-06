# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys

from typing import Any

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetsToStatesMachine,
)
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.generic_syncer_metrics import (
    report_assets_data_batch_size_bytes,
    report_assets_data_fetch_error,
    report_get_assets_data_response_time,
    report_save_tracking_data_response_time,
    report_sync_once_batch_duration_seconds,
    report_total_assets_size,
)
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from dbnd._core.utils.timezone import utcnow


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
        self.syncer_instance_id = syncer_instance_id
        self.assets_to_states_machine = AssetsToStatesMachine(
            integration_id=self.config.uid,
            syncer_instance_id=self.syncer_instance_id,
            max_retries=config.syncer_max_retries,
        )

    def _sync_once(self):
        cursor = self._get_or_init_cursor()

        active_assets = self._get_active_assets()
        synced_active_data = self._process_assets_batch(active_assets)

        new_assets = self._get_new_assets_and_update_cursor(cursor)
        synced_new_data = self._process_assets_batch(new_assets)

        self.integration_management_service.report_monitor_time_data(
            self.config.uid, synced_new_data=(synced_active_data or synced_new_data)
        )

    def _get_active_assets(self) -> Assets:
        active_assets_to_states = self.tracking_service.get_active_assets(
            integration_id=str(self.config.uid),
            syncer_instance_id=self.syncer_instance_id,
        )
        update_assets = Assets(assets_to_state=active_assets_to_states)
        return update_assets

    def _get_new_assets_and_update_cursor(self, cursor: Any) -> Assets:
        new_assets, new_cursor = self.adapter.get_new_assets_for_cursor(cursor)
        if new_assets.assets_to_state:
            self.tracking_service.save_assets_state(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                assets_to_state=new_assets.assets_to_state,
            )
            report_total_assets_size(
                integration_id=self.config.uid,
                syncer_instance_id=self.syncer_instance_id,
                assets_size=len(new_assets.assets_to_state),
            )
        if new_cursor and new_cursor != cursor:
            # report last cursor only when all pages saved
            self.tracking_service.update_last_cursor(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                state="update",
                data=new_cursor,
            )
        return new_assets

    def _get_or_init_cursor(self) -> Any:
        cursor = self.tracking_service.get_last_cursor(
            integration_id=str(self.config.uid),
            syncer_instance_id=self.syncer_instance_id,
        )
        if cursor is None:
            cursor = self.adapter.init_cursor()
            self.tracking_service.update_last_cursor(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                state="init",
                data=cursor,
            )
        return cursor

    def _process_assets_batch(self, assets: Assets) -> bool:
        batch_process_start_time = utcnow()
        synced_data = False
        try:
            assets_data = self.adapter.get_assets_data(assets)
        except Exception as ex:
            logger.error("Error on fetching assets data: %s", str(ex))
            report_assets_data_fetch_error(
                integration_id=self.config.uid,
                syncer_instance_id=self.syncer_instance_id,
                error_message=str(ex),
            )
            return synced_data
        get_assets_data_duration = (utcnow() - batch_process_start_time).total_seconds()
        report_get_assets_data_response_time(
            integration_id=self.config.uid,
            syncer_instance_id=self.syncer_instance_id,
            duration=get_assets_data_duration,
        )
        assets_data_to_report = assets_data.data
        assets_states_to_report = assets_data.assets_to_state
        if not assets_data_to_report:
            logger.info(
                "No new assets data found for tracking source %s", self.server_id
            )
        else:
            logger.info("Found new assets data for tracking source %s", self.server_id)
            synced_data = True
            try:
                assets_data_batch_size = sys.getsizeof(assets_data_to_report)
                report_assets_data_batch_size_bytes(
                    integration_id=self.config.uid,
                    syncer_instance_id=self.syncer_instance_id,
                    assets_data_batch_size_bytes=assets_data_batch_size,
                )
            except TypeError:
                logger.warning("assets data batch size could not be calculated")
            save_tracking_data_start_time = utcnow()
            self.tracking_service.save_tracking_data(assets_data_to_report)
            save_tracking_data_end_time = utcnow()
            save_tracking_data_duration_seconds = (
                save_tracking_data_end_time - save_tracking_data_start_time
            ).total_seconds()
            report_save_tracking_data_response_time(
                integration_id=self.config.uid,
                syncer_instance_id=self.syncer_instance_id,
                duration=save_tracking_data_duration_seconds,
            )
        if not assets_states_to_report:
            logger.info("No new assets states for tracking source %s", self.server_id)
        else:
            logger.info(
                "Found new assets states for tracking source %s", self.server_id
            )
            self.tracking_service.save_assets_state(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                assets_to_state=self.assets_to_states_machine.process(
                    assets_states_to_report
                ),
            )
        batch_process_end_time = utcnow()
        batch_process_duration_seconds = (
            batch_process_end_time - batch_process_start_time
        ).total_seconds()
        report_sync_once_batch_duration_seconds(
            integration_id=self.config.uid,
            syncer_instance_id=self.syncer_instance_id,
            duration=batch_process_duration_seconds,
        )
        return synced_data

    @property
    def identifier(self) -> str:
        return self.SYNCER_TYPE + "_" + self.syncer_instance_id
