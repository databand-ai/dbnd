# © Copyright Databand.ai, an IBM Company 2022
import logging

from datetime import timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlparse

import more_itertools

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_runs_error_handler.datastage_runs_error_handler import (
    DatastageRunRequestsRetryQueue,
)
from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)
from dbnd_datastage_monitor.metrics.prometheus_metrics import (
    report_in_progress_failed_run_request_skipped,
    report_list_duration,
    report_run_request_retry_cache_size,
    report_run_request_retry_delay,
    report_run_request_retry_fetched_from_error_queue,
    report_run_request_retry_queue_size,
    report_run_request_retry_submitted_to_error_queue,
    report_runs_collection_delay,
    report_runs_not_initiated,
)
from dbnd_datastage_monitor.tracking_service.datastage_syncer_management_service import (
    DataStageSyncersManagementService,
)
from dbnd_datastage_monitor.tracking_service.datastage_tracking_service import (
    DataStageTrackingService,
)

from airflow_monitor.shared.base_component import BaseComponent
from dbnd._core.utils.date_utils import parse_datetime
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


def get_from_nullable_chain(
    source: Any, chain: List[str], default_val=None
) -> Optional[Any]:
    # This function is taken from dbnd_web.utils.operations
    chain.reverse()
    try:
        while chain:
            next_key = chain.pop()
            if isinstance(source, dict):
                source = source.get(next_key)
            else:
                source = getattr(source, next_key)
        return source
    except AttributeError:
        return default_val


def format_datetime(datetime_obj):
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_project_id_from_url(url: str):
    parsed_url = urlparse(url)
    parsed_query_string = dict(parse_qsl(parsed_url.query))
    project_id = parsed_query_string.get("project_id")
    return project_id


class DataStageRunsSyncer(BaseComponent):
    SYNCER_TYPE = "datastage_runs_syncer"

    tracking_service: DataStageTrackingService
    config: DataStageServerConfig
    data_fetcher: MultiProjectDataStageDataFetcher

    def __init__(
        self,
        config: DataStageServerConfig,
        tracking_service: DataStageTrackingService,
        syncer_management_service: DataStageSyncersManagementService,
        data_fetcher: object,
    ):
        super(DataStageRunsSyncer, self).__init__(
            config, tracking_service, syncer_management_service, data_fetcher
        )
        self.error_handler = DatastageRunRequestsRetryQueue(
            tracking_source_uid=self.config.tracking_source_uid
        )

    def refresh_config(self, config):
        super(DataStageRunsSyncer, self).refresh_config(config)
        from dbnd_datastage_monitor.multiserver.datastage_services_factory import (
            DataStageMonitorServicesFactory,
        )

        datastage_asset_clients = DataStageMonitorServicesFactory.get_asset_clients(
            self.config
        )
        self.data_fetcher.update_projects(datastage_asset_clients)

    def _sync_once(self):
        logger.info(
            "Started running for tracking source %s", self.config.tracking_source_uid
        )
        report_run_request_retry_queue_size(
            self.config.tracking_source_uid,
            self.error_handler.get_run_request_retries_queue_size(),
        )
        report_run_request_retry_cache_size(
            self.config.tracking_source_uid,
            self.error_handler.get_run_request_retries_cache_size(),
        )
        last_seen_date_str = self.tracking_service.get_last_seen_date()

        if not last_seen_date_str:
            self.tracking_service.update_last_seen_values(format_datetime(utcnow()))
            return

        running_datastage_runs = self.tracking_service.get_running_datastage_runs()
        self.update_runs(running_datastage_runs)

        current_date = utcnow()
        interval = timedelta(self.config.fetching_interval_in_minutes)
        start_date = end_date = parse_datetime(last_seen_date_str)

        duration = (current_date - start_date).total_seconds()
        report_list_duration(self.config.tracking_source_uid, duration)

        while end_date < current_date:
            end_date = (
                start_date + interval
                if start_date + interval < current_date
                else current_date
            )
            logger.info(
                "Checking for new runs from %s to %s for tracking source %s",
                format_datetime(start_date),
                format_datetime(end_date),
                self.config.tracking_source_uid,
            )
            new_datastage_runs: Dict[
                str, Dict[str, str]
            ] = self.data_fetcher.get_runs_to_sync(
                format_datetime(start_date), format_datetime(end_date)
            )
            # check for failed runs to submit for retry
            new_datastage_runs: Dict[
                str, Dict[str, str]
            ] = self._append_failed_run_requests_for_retry(new_datastage_runs)
            for p in new_datastage_runs:
                logger.info(
                    "Found %d new runs for project %s for tracking source %s",
                    len(new_datastage_runs[p]),
                    p,
                    self.config.tracking_source_uid,
                )

            has_new_run = [v for v in new_datastage_runs.values() if v]
            if has_new_run:
                runs_to_submit: Dict[str, List] = {
                    k: list(v.values()) for k, v in new_datastage_runs.items() if v
                }
                if runs_to_submit:
                    self.init_runs(runs_to_submit)
                    self.tracking_service.update_last_seen_values(
                        format_datetime(end_date)
                    )
                    self.syncer_management_service.update_last_sync_time(
                        self.config.identifier
                    )
            else:
                logger.info(
                    "No new runs found for tracking source %s",
                    self.config.tracking_source_uid,
                )
                self.syncer_management_service.update_last_sync_time(
                    self.config.identifier
                )

            start_date += interval

    def _append_failed_run_requests_for_retry(
        self, datastage_runs: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        failed_run_requests_to_retry = self.error_handler.pull_run_request_retries(
            batch_size=10
        )
        if failed_run_requests_to_retry:
            new_datastage_runs = datastage_runs.copy()
            logger.warning(
                "submitting %s failed runs to retry", len(failed_run_requests_to_retry)
            )
            for failed_run_request in failed_run_requests_to_retry:
                project_id = failed_run_request.project_id
                report_run_request_retry_fetched_from_error_queue(
                    tracking_source_uid=self.config.tracking_source_uid,
                    project_uid=project_id,
                )
                if project_id in new_datastage_runs:
                    new_datastage_runs.get(project_id)[
                        failed_run_request.run_id
                    ] = failed_run_request.run_link
                else:
                    new_datastage_runs[project_id] = {
                        failed_run_request.run_id: failed_run_request.run_link
                    }
            return new_datastage_runs
        else:
            logger.debug("no failed runs to retry")
            return datastage_runs

    def init_runs(self, datastage_runs: Dict[str, List[str]]):
        if not datastage_runs:
            return

        for project_id, runs in datastage_runs.items():
            logger.info(
                "Syncing new %d runs for project %s of tracking source %s",
                len(runs),
                project_id,
                self.config.tracking_source_uid,
            )
            successful_run_inits = 0
            all_runs = []

            bulk_size = self.config.runs_bulk_size or len(runs)
            chunks = more_itertools.sliced(runs, bulk_size)
            for runs_chunk in chunks:
                (
                    datastage_runs_full_data,
                    failed_run_requests,
                ) = self.data_fetcher.get_full_runs(runs_chunk, project_id)
                if failed_run_requests:
                    logger.warning(
                        "%s failed run requests found", len(failed_run_requests)
                    )
                    self.error_handler.submit_run_request_retries(failed_run_requests)
                    report_run_request_retry_submitted_to_error_queue(
                        tracking_source_uid=self.config.tracking_source_uid,
                        project_uid=project_id,
                        number_of_runs=len(failed_run_requests),
                    )
                if datastage_runs_full_data:
                    received_runs = datastage_runs_full_data.get("runs")
                    if received_runs:
                        all_runs.extend(received_runs)
                        successful_run_inits += len(received_runs)
                    self.tracking_service.init_datastage_runs(datastage_runs_full_data)

            if successful_run_inits < len(runs):
                report_runs_not_initiated(
                    self.config.tracking_source_uid,
                    project_id,
                    len(runs) - successful_run_inits,
                )

            current_date = utcnow()
            min_start_time = self.get_min_run_start_time(all_runs, current_date)
            if min_start_time:
                collection_delay = (current_date - min_start_time).total_seconds()
                report_runs_collection_delay(
                    self.config.tracking_source_uid, project_id, collection_delay
                )

    def update_runs(self, run_ids_to_update: List):
        if not run_ids_to_update:
            return

        run_partitioned_by_project_id = {}
        for run_link in run_ids_to_update:
            project_id = _extract_project_id_from_url(run_link)
            project_runs = run_partitioned_by_project_id.setdefault(project_id, [])
            project_runs.append(run_link)

        for project_id, runs in run_partitioned_by_project_id.items():
            logger.info(
                "Updating %d runs for project %s for tracking source %s",
                len(runs),
                project_id,
                self.config.tracking_source_uid,
            )

            bulk_size = self.config.runs_bulk_size or len(runs)
            chunks = more_itertools.sliced(runs, bulk_size)
            for runs_chunk in chunks:
                # do not submit failed runs on update, already reported to server as in progress
                (
                    datastage_runs_full_data,
                    failed_run_requests,
                ) = self.data_fetcher.get_full_runs(runs_chunk, project_id)
                if failed_run_requests:
                    skipped_failed_run_requests_len = len(failed_run_requests)
                    logger.warning(
                        "%s failed run requests found while trying to update run",
                        skipped_failed_run_requests_len,
                    )
                    report_in_progress_failed_run_request_skipped(
                        self.config.tracking_source_uid,
                        project_id,
                        skipped_failed_run_requests_len,
                    )
                    for failed_run_request in failed_run_requests:
                        logger.warning(
                            "skipping failed run request %s for retry",
                            failed_run_request,
                        )
                self.tracking_service.update_datastage_runs(datastage_runs_full_data)
                self.syncer_management_service.update_last_sync_time(
                    self.config.identifier
                )

    def get_min_run_start_time(self, raw_runs, current_date):
        min_start_time = None
        for raw_run in raw_runs:
            try:
                start_time = parse_datetime(
                    get_from_nullable_chain(
                        raw_run.get("run_info"), ["metadata", "created_at"]
                    )
                )
                if self.error_handler.is_run_retry(raw_run.get("run_link")):
                    self.report_run_request_retry_duration(
                        current_date, raw_run, start_time
                    )
                # min start time for regular runs in collection
                elif min_start_time is None or start_time < min_start_time:
                    min_start_time = start_time
            except Exception as e:
                logger.exception(
                    "Failed to get start time from run, exception: %s", str(e)
                )

        return min_start_time

    def report_run_request_retry_duration(self, current_date, raw_run, start_time):
        if start_time:
            retry_run_delay = (current_date - start_time).total_seconds()
            run_uid = get_from_nullable_chain(
                raw_run.get("run_info"), ["metadata", "asset_id"]
            )
            report_run_request_retry_delay(
                self.config.tracking_source_uid, run_uid, retry_run_delay
            )
