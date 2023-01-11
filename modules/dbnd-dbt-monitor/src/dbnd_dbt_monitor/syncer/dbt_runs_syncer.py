# © Copyright Databand.ai, an IBM Company 2022

import logging

import more_itertools

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.tracking_service.dbt_tracking_service import DbtTrackingService

from airflow_monitor.shared.base_component import BaseComponent


logger = logging.getLogger(__name__)


class DbtRunsSyncer(BaseComponent):
    SYNCER_TYPE = "dbt_runs_syncer"

    tracking_service: DbtTrackingService
    config: DbtServerConfig
    data_fetcher: DbtCloudDataFetcher

    def _sync_once(self):
        dbnd_last_run_id = self.tracking_service.get_last_seen_run_id()

        if not dbnd_last_run_id:
            dbt_last_run_id = self.data_fetcher.get_last_seen_run_id()
            if dbt_last_run_id:
                self.tracking_service.update_last_seen_values(dbt_last_run_id)
            return

        running_dbt_run_ids = self.tracking_service.get_running_dbt_run_ids()
        self.update_runs(running_dbt_run_ids)

        new_dbt_run_ids = self.data_fetcher.get_run_ids_to_sync_from_dbt(
            dbnd_last_run_id
        )

        if new_dbt_run_ids:
            self.init_runs(new_dbt_run_ids)
            self._update_last_seen_values(new_dbt_run_ids)

    def init_runs(self, run_ids_to_init):
        if not run_ids_to_init:
            return

        logger.info("Syncing new %d runs", len(run_ids_to_init))

        bulk_size = self.config.runs_bulk_size or len(run_ids_to_init)
        chunks = more_itertools.sliced(run_ids_to_init, bulk_size)
        for runs_chunk in chunks:
            dbt_runs_full_data = self.data_fetcher.get_full_dbt_runs(runs_chunk)
            if dbt_runs_full_data:
                self.tracking_service.init_dbt_runs(dbt_runs_full_data)

    def update_runs(self, run_ids_to_update):
        if not run_ids_to_update:
            return

        logger.info("Updating %d runs", len(run_ids_to_update))

        bulk_size = self.config.runs_bulk_size or len(run_ids_to_update)
        chunks = more_itertools.sliced(run_ids_to_update, bulk_size)
        for runs_chunk in chunks:
            dbt_runs_full_data = self.data_fetcher.get_full_dbt_runs(runs_chunk)
            if dbt_runs_full_data:
                self.tracking_service.update_dbt_runs(dbt_runs_full_data)

    def _update_last_seen_values(self, dbt_run_ids):
        if not dbt_run_ids:
            return

        new_last_sees_run_id = max(dbt_run_ids)
        self.tracking_service.update_last_seen_values(new_last_sees_run_id)
