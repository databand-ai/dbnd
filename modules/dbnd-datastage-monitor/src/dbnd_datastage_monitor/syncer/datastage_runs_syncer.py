# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from typing import Dict, List
from urllib.parse import parse_qsl, urlparse

import more_itertools

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)
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


def _extract_project_id_from_url(url: str):
    parsed_url = urlparse(url)
    parsed_query_string = dict(parse_qsl(parsed_url.query))
    project_id = parsed_query_string.get("project_id")
    return project_id


class DataStageRunsSyncer(BaseMonitorSyncer):
    SYNCER_TYPE = "datastage_runs_syncer"

    tracking_service: DbndDataStageTrackingService
    config: DataStageServerConfig
    data_fetcher: MultiProjectDataStageDataFetcher

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        last_seen_date_str = self.tracking_service.get_last_seen_date()

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

            new_datastage_runs: Dict[
                str, Dict[str, str]
            ] = self.data_fetcher.get_runs_to_sync(
                format_datetime(start_date), format_datetime(end_date)
            )
            has_new_run = [v for v in new_datastage_runs.values() if v]
            if has_new_run:
                runs_to_submit: Dict[str, List] = {
                    k: list(v.values()) for k, v in new_datastage_runs.items() if v
                }
                self._init_runs_for_project(runs_to_submit)
                self.tracking_service.update_last_seen_values(format_datetime(end_date))
                self.tracking_service.update_last_sync_time()
            else:
                logger.info("No new runs found")
                self.tracking_service.update_last_sync_time()

            start_date += interval

    @capture_monitor_exception
    def _init_runs_for_project(self, datastage_runs: Dict[str, List[str]]):
        if not datastage_runs:
            return
        for project_id, runs in datastage_runs.items():
            logger.info("Syncing new %d runs for project %s", len(runs), project_id)

            bulk_size = self.config.runs_bulk_size or len(runs)
            chunks = more_itertools.sliced(runs, bulk_size)
            for runs_chunk in chunks:
                datastage_runs_full_data = self.data_fetcher.get_full_runs(
                    runs_chunk, project_id
                )
                self.tracking_service.init_datastage_runs(datastage_runs_full_data)

    @capture_monitor_exception
    def _update_runs(self, datastage_runs: List):
        if not datastage_runs:
            return

        run_partitioned_by_project_id = {}
        for run_link in datastage_runs:
            project_id = _extract_project_id_from_url(run_link)
            project_runs = run_partitioned_by_project_id.setdefault(project_id, [])
            project_runs.append(run_link)

        for project_id, runs in run_partitioned_by_project_id.items():
            logger.info("Updating %d runs for project %s", len(runs), project_id)

            bulk_size = self.config.runs_bulk_size or len(runs)
            chunks = more_itertools.sliced(runs, bulk_size)
            for runs_chunk in chunks:
                datastage_runs_full_data = self.data_fetcher.get_full_runs(
                    runs_chunk, project_id
                )
                self.tracking_service.update_datastage_runs(datastage_runs_full_data)
                self.tracking_service.update_last_sync_time()
