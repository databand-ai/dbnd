# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from typing import Dict, List

import more_itertools

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.fetcher.datastage_data_fetcher import DataStageDataFetcher
from dbnd_datastage_monitor.multiserver.datastage_services_factory import (
    get_datastage_services_factory,
)
from dbnd_datastage_monitor.tracking_service.dbnd_datastage_tracking_service import (
    DbndDataStageTrackingService,
)

from airflow_monitor.common.base_component import BaseMonitorSyncer
from airflow_monitor.shared.error_handler import CaptureMonitorExceptionDecorator
from dbnd._core.utils.date_utils import parse_datetime
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)

capture_monitor_exception = CaptureMonitorExceptionDecorator(
    configuration_service_provider=get_datastage_services_factory().get_servers_configuration_service
)


def format_datetime(datetime_obj):
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


class DataStageRunsSyncer(BaseMonitorSyncer):
    SYNCER_TYPE = "datastage_runs_syncer"

    tracking_service: DbndDataStageTrackingService
    config: DataStageServerConfig
    data_fetcher: DataStageDataFetcher

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        last_seen_date_str = self.tracking_service.get_lat_seen_date()

        if not last_seen_date_str:
            self.tracking_service.update_last_seen_values(format_datetime(utcnow()))
            return

        running_datastage_runs = self.tracking_service.get_running_datastage_runs()
        self._update_runs(running_datastage_runs)

        current_date = utcnow()
        interval = timedelta(self.config.fetching_interval_in_minutes)
        start_date = end_date = parse_datetime(last_seen_date_str)

        while end_date < current_date:
            end_date = (
                start_date + interval
                if start_date + interval < current_date
                else current_date
            )

            new_datastage_runs: Dict[str, str] = self.data_fetcher.get_runs_to_sync(
                format_datetime(start_date), format_datetime(end_date)
            )
            if new_datastage_runs:
                self._init_runs(list(new_datastage_runs.values()))
                self.tracking_service.update_last_seen_values(format_datetime(end_date))

            start_date += interval

    @capture_monitor_exception
    def _init_runs(self, datastage_runs: List):
        if not datastage_runs:
            return

        logger.info("Syncing new %d runs", len(datastage_runs))

        bulk_size = self.config.runs_bulk_size or len(datastage_runs)
        chunks = more_itertools.sliced(datastage_runs, bulk_size)
        for runs_chunk in chunks:
            datastage_runs_full_data = self.data_fetcher.get_full_runs(runs_chunk)
            self.tracking_service.init_datastage_runs(datastage_runs_full_data)

    @capture_monitor_exception
    def _update_runs(self, datastage_runs: List):
        if not datastage_runs:
            return

        logger.info("Updating %d runs", len(datastage_runs))

        bulk_size = self.config.runs_bulk_size or len(datastage_runs)
        chunks = more_itertools.sliced(datastage_runs, bulk_size)
        for runs_chunk in chunks:
            datastage_runs_full_data = self.data_fetcher.get_full_runs(runs_chunk)
            self.tracking_service.update_datastage_runs(datastage_runs_full_data)
