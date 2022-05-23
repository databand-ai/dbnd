import logging

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.syncer.base_syncer import start_syncer
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.base_component import BaseMonitorSyncer


logger = logging.getLogger(__name__)


class DbtRunsSyncer(BaseMonitorSyncer):
    SYNCER_TYPE = "dbt_runs_syncer"

    tracking_service: DbndDbtTrackingService
    config: DbtServerConfig
    data_fetcher: DbtCloudDataFetcher

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        dbnd_last_run_id = self.tracking_service.get_last_seen_run_id()

        if not dbnd_last_run_id:
            dbt_last_run_id = self.data_fetcher.get_last_seen_run_id()
            dbnd_last_run_id = dbt_last_run_id

        new_dbt_run_ids = self.data_fetcher.get_run_ids_to_sync_from_dbt(
            dbnd_last_run_id
        )
        self._init_runs(new_dbt_run_ids)
        self._update_last_seen_values(new_dbt_run_ids)

        running_dbt_run_ids = self.tracking_service.get_running_dbt_run_ids()
        self._update_runs(running_dbt_run_ids)

    @capture_monitor_exception
    def _init_runs(self, dbt_run_ids):
        if not dbt_run_ids:
            return

        bulk_size = self.config.runs_bulk_size or len(dbt_run_ids)
        for i in range(0, len(dbt_run_ids), bulk_size):
            runs_chunk = dbt_run_ids[i : i + bulk_size]
            dbt_runs_full_data = self.data_fetcher.get_full_dbt_runs(runs_chunk)
            self.tracking_service.init_dbt_runs(dbt_runs_full_data)

    @capture_monitor_exception
    def _update_runs(self, dbt_run_ids):
        if not dbt_run_ids:
            return

        bulk_size = self.config.runs_bulk_size or len(dbt_run_ids)
        for i in range(0, len(dbt_run_ids), bulk_size):
            runs_chunk = dbt_run_ids[i : i + bulk_size]
            dbt_runs_full_data = self.data_fetcher.get_full_dbt_runs(runs_chunk)
            self.tracking_service.update_dbt_runs(dbt_runs_full_data)

    def _update_last_seen_values(self, dbt_run_ids):
        if not dbt_run_ids:
            return

        new_last_sees_run_id = max(dbt_run_ids)
        self.tracking_service.update_last_seen_values(new_last_sees_run_id)


def start_dbt_runs_syncer(tracking_source_uid, run=True):
    return start_syncer(DbtRunsSyncer, tracking_source_uid=tracking_source_uid, run=run)
