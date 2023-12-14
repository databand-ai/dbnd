# © Copyright Databand.ai, an IBM Company 2022
import logging

from collections import Counter
from typing import Any, Collection, List, Tuple

import attr

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetState,
    AssetToState,
    update_assets_retry_state,
)
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_integration_config import BaseIntegrationConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.integration_metrics_reporter import (
    IntegrationMetricsReporter,
)
from airflow_monitor.shared.reporting_service import ReportingService


logger = logging.getLogger(__name__)


def mark_assets_as_failed(assets: List[AssetToState]) -> List[AssetToState]:
    return [attr.evolve(asset, state=AssetState.FAILED_REQUEST) for asset in assets]


def add_missing_assets_as_failed(
    actual_assets: List[AssetToState], expected_assets: List[AssetToState]
) -> List[AssetToState]:
    """
    Adds all assets from `expected_assets` which does not exist in `actual_assets`,
    and marks them as failed
    """
    actual_assets_by_asset_id = {asset.asset_id: asset for asset in actual_assets}

    # report all missing as failed
    missing_assets = [
        expected_asset
        for expected_asset in expected_assets
        if expected_asset.asset_id not in actual_assets_by_asset_id
    ]

    if not missing_assets:
        # All good
        return actual_assets

    logger.warning(
        "Missing assets will be marked as failed, asset_ids=%s",
        assets_to_str(missing_assets),
    )
    return actual_assets + mark_assets_as_failed(missing_assets)


def split_failed_and_not_failed(
    assets: List[AssetToState],
) -> Tuple[List[AssetToState], List[AssetToState]]:
    non_failed_assets, failed_assets = [], []
    for asset in assets:
        if asset.state == AssetState.FAILED_REQUEST:
            failed_assets.append(asset)
        else:
            non_failed_assets.append(asset)
    return failed_assets, non_failed_assets


def assets_to_str(assets_to_state: List[AssetToState]) -> str:
    if not assets_to_state:
        return str(assets_to_state)

    return ",".join(str(asset.asset_id) for asset in assets_to_state)


def get_data_dimension_str(data: Any) -> str:
    """
    Returns asset data dimension. Example:
    > get_data_dimension_str({"runs": [1,2,3], "jobs": {"hello": ..., "world": ...,}
    runs:3, jobs:2
    """

    def safe_len(val: Any) -> int:
        return len(val) if isinstance(val, Collection) else 1

    if isinstance(data, dict):
        return ", ".join(f"{key}:{safe_len(value)}" for key, value in data.items())
    return str(safe_len(data)) if data else str(data)


class GenericSyncer(BaseComponent):
    SYNCER_TYPE = "generic_syncer"

    tracking_service: BaseTrackingService
    config: BaseIntegrationConfig
    adapter: Adapter
    syncer_instance_id: str

    def __init__(
        self,
        config: BaseIntegrationConfig,
        tracking_service: BaseTrackingService,
        reporting_service: ReportingService,
        adapter: Adapter,
        syncer_instance_id: str,
    ):
        super(GenericSyncer, self).__init__(
            config, tracking_service, reporting_service, None
        )
        self.adapter = adapter
        self.syncer_instance_id = syncer_instance_id
        self.metrics_reporter = IntegrationMetricsReporter(
            integration_id=str(self.config.uid),
            tracking_source_uid=self.tracking_service.tracking_source_uid,
            syncer_type=self.config.source_type,
        )

    def _sync_once(self):
        cursor = self._get_or_init_cursor()

        active_assets = self._get_active_assets()
        self.process_assets_in_chunks(active_assets)
        logger.info("Finished collecting and processing active assets")

        new_assets = self._get_new_assets_and_update_cursor(cursor)
        self.process_assets_in_chunks(new_assets)
        logger.info("Finished collecting and processing new assets")

        self.reporting_service.report_monitor_time_data(
            self.config.uid, synced_new_data=False
        )

    def _get_active_assets(self) -> Assets:
        active_assets_to_states = self.tracking_service.get_active_assets(
            integration_id=str(self.config.uid),
            syncer_instance_id=self.syncer_instance_id,
        )
        if active_assets_to_states:
            self.metrics_reporter.report_total_assets_size(
                asset_source="get_active_assets",
                asset_count=len(active_assets_to_states),
            )
            logger.info(
                "_get_active_assets collected %d active assets",
                len(active_assets_to_states),
            )
        update_assets = Assets(assets_to_state=active_assets_to_states)
        return update_assets

    def _get_new_assets_and_update_cursor(self, cursor: Any) -> Assets:
        # We do not catch exceptions here, so that if there is a real error getting data
        # it will get to capture_component_exception and sent to the webserver.
        new_assets, new_cursor = self.adapter.get_new_assets_for_cursor(cursor)
        if new_assets.assets_to_state:
            self.tracking_service.save_assets_state(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                assets_to_state=new_assets.assets_to_state,
            )
            self.metrics_reporter.report_total_assets_size(
                asset_source="get_new_assets",
                asset_count=len(new_assets.assets_to_state),
            )
        if new_cursor and new_cursor != cursor:
            # report last cursor only when all pages saved
            self.tracking_service.update_last_cursor(
                integration_id=str(self.config.uid),
                syncer_instance_id=self.syncer_instance_id,
                state="update",
                data=new_cursor,
            )
        if new_assets.assets_to_state:
            logger.info(
                "_get_new_assets_and_update_cursor collected %d active assets",
                len(new_assets.assets_to_state),
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

    def process_assets_in_chunks(self, assets: Assets) -> None:
        if not assets.assets_to_state:
            return

        failed, non_failed = split_failed_and_not_failed(assets.assets_to_state)
        logger.info(
            "Processing assets in chunks failed=%s not_failed=%s",
            len(failed),
            len(non_failed),
        )

        bulk_size = self.config.sync_bulk_size
        for i in range(0, len(non_failed), bulk_size):
            assets_chunk = non_failed[i : i + bulk_size]
            self._process_assets_batch(
                attr.evolve(assets, assets_to_state=assets_chunk)
            )

        # process failed assets one-by-one, to prevent 1 bad asset failing all the rest
        for asset in failed:
            self._process_assets_batch(attr.evolve(assets, assets_to_state=[asset]))

    def _process_assets_batch(self, assets_to_process: Assets) -> None:
        try:
            with self.metrics_reporter.execution_time("get_assets_data").time():
                assets_data = self.adapter.get_assets_data(assets_to_process)

            if assets_data.data:
                with self.metrics_reporter.execution_time("save_tracking_data").time():
                    self.tracking_service.save_tracking_data(assets_data.data)

                logger.info(
                    "Saved new assets data for tracking source %s, asset_ids=%s data=%s",
                    self.server_id,
                    assets_to_str(assets_data.assets_to_state),
                    get_data_dimension_str(assets_data.data),
                )
                self.reporting_service.report_monitor_time_data(
                    self.config.uid, synced_new_data=True
                )
            else:
                logger.info("No assets to save - _process_assets_batch")

            new_assets_states = assets_data.assets_to_state
        except Exception:
            logger.exception(
                "Unexpected error while processing assets batch, asset_ids=%s",
                assets_to_str(assets_to_process.assets_to_state),
                exc_info=True,
            )
            # if we were able to get_assets_data but weren't able to save_tracking_data
            # we should still consider all assets as failed
            new_assets_states = None

        new_assets_states = add_missing_assets_as_failed(
            new_assets_states or [], assets_to_process.assets_to_state
        )

        # Here we take care of retry count, transition to MAX_RETRIES, etc.
        new_assets_states = update_assets_retry_state(
            new_assets_states, max_retries=self.config.syncer_max_retries
        )
        self.report_assets_metrics(new_assets_states)

        self.tracking_service.save_assets_state(
            integration_id=str(self.config.uid),
            syncer_instance_id=self.syncer_instance_id,
            assets_to_state=new_assets_states,
        )

    def report_assets_metrics(self, new_assets_states: List[AssetToState]) -> None:
        asset_states = Counter([a.state for a in new_assets_states])
        for state, count in asset_states.items():
            self.metrics_reporter.report_assets_in_state(
                asset_state=state.value, count=count
            )

    @property
    def identifier(self) -> str:
        return self.SYNCER_TYPE + "_" + self.syncer_instance_id
