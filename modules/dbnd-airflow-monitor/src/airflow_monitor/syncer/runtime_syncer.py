# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

from airflow_monitor.common.airflow_data import (
    AirflowDagRun,
    AirflowDagRunsResponse,
    DagRunsStateData,
)
from airflow_monitor.common.base_component import BaseMonitorSyncer, start_syncer
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.errors import capture_monitor_exception
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.tracking_service.web_tracking_service import (
    AirflowDbndTrackingService,
)


logger = logging.getLogger(__name__)

VIEW_ONLY_EVENTS = {
    "duration",
    "elasticsearch",
    "extra_links",
    "gantt",
    "get_logs_with_metadata",
    "graph",
    "landing_times",
    "log",
    "rendered",
    "task",
    "task_instances",
    "tree",
    "tries",
    "variables",
    "varimport",
    "xcom",
}


def is_view_only_events(dr: AirflowDagRun):
    if not dr.events:
        return False
    non_view_events = set(dr.events.split(",")) - VIEW_ONLY_EVENTS
    return len(non_view_events) == 0


def categorize_dag_runs(
    airflow_dag_runs: List[AirflowDagRun], dbnd_dag_run_ids: List[int]
):
    dagruns_to_init = []
    dagruns_to_update = []
    dagruns_to_skip = []
    for dr in airflow_dag_runs:
        is_finished = dr.state != "RUNNING"
        if is_finished and is_view_only_events(dr):
            dagruns_to_update.append(dr)
        elif dr.id not in dbnd_dag_run_ids:
            dagruns_to_init.append(dr)
        elif is_finished or dr.has_updated_task_instances or dr.is_paused:
            dagruns_to_update.append(dr)
        else:
            dagruns_to_skip.append(dr)
    return dagruns_to_init, dagruns_to_update


class AirflowRuntimeSyncer(BaseMonitorSyncer):
    SYNCER_TYPE = "runtime_syncer"

    tracking_service: AirflowDbndTrackingService
    config: AirflowServerConfig
    data_fetcher: AirflowDataFetcher

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        dbnd_response = self.tracking_service.get_active_dag_runs(
            start_time_window=self.config.start_time_window, dag_ids=self.config.dag_ids
        )
        if (
            dbnd_response.last_seen_dag_run_id is None
            or dbnd_response.last_seen_log_id is None
        ):
            last_seen_values = self.data_fetcher.get_last_seen_values()
            if last_seen_values.last_seen_log_id is None:
                last_seen_values.last_seen_log_id = -1
            if last_seen_values.last_seen_dag_run_id is None:
                last_seen_values.last_seen_dag_run_id = -1

            self.tracking_service.update_last_seen_values(last_seen_values)

            dbnd_response = self.tracking_service.get_active_dag_runs(
                start_time_window=self.config.start_time_window,
                dag_ids=self.config.dag_ids,
            )

        logger.debug(
            "Getting new dag runs from Airflow with parameters last_seen_dag_run_id=%s, last_seen_log_id=%s, extra_dag_run_ids=%s, dag_ids=%s",
            dbnd_response.last_seen_dag_run_id,
            dbnd_response.last_seen_log_id,
            ",".join(map(str, dbnd_response.dag_run_ids)),
            self.config.dag_ids,
        )
        airflow_response = self.data_fetcher.get_airflow_dagruns_to_sync(
            last_seen_dag_run_id=dbnd_response.last_seen_dag_run_id,
            last_seen_log_id=dbnd_response.last_seen_log_id,
            extra_dag_run_ids=dbnd_response.dag_run_ids,
            dag_ids=self.config.dag_ids,
        )  # type: AirflowDagRunsResponse

        dagruns_to_init, dagruns_to_update = categorize_dag_runs(
            airflow_response.dag_runs, dbnd_response.dag_run_ids
        )

        self.init_dagruns(dagruns_to_init)
        self.update_dagruns(dagruns_to_update)

    @capture_monitor_exception
    def init_dagruns(self, dagruns: List[AirflowDagRun]):
        if not dagruns:
            return

        plugin_metadata = self.data_fetcher.get_plugin_metadata()
        dagruns = sorted(dagruns, key=lambda dr: dr.id)  # type: List[AirflowDagRun]

        bulk_size = self.config.dag_run_bulk_size or len(dagruns)
        for i in range(0, len(dagruns), bulk_size):
            dagruns_chunk = dagruns[i : i + bulk_size]
            dag_run_ids = [dr.id for dr in dagruns_chunk]
            dag_runs_full_data = self.data_fetcher.get_full_dag_runs(
                dag_run_ids, self.config.include_sources
            )
            logger.info(
                "Syncing new %d dag runs and %d task instances",
                len(dag_runs_full_data.dag_runs),
                len(dag_runs_full_data.task_instances),
            )
            self.tracking_service.init_dagruns(
                dag_runs_full_data, max(dag_run_ids), self.SYNCER_TYPE, plugin_metadata
            )

    @capture_monitor_exception
    def update_dagruns(self, dagruns: List[AirflowDagRun]):
        # update_dagruns will fetch updated states for given dagruns and send them
        # to webserver (in chunks). As a side effect - webserver will also update
        # last_sync_time which is used as heartbeat. We do it as part of update_dagruns
        # to be sure that we update last_sync_time only if data is being updated.
        if not dagruns:
            # if there is no dagruns to update - do empty update to set last_sync_time
            # otherwise we might get false alarms that monitor is not syncing
            self.tracking_service.update_dagruns(
                DagRunsStateData(task_instances=[], dag_runs=[]), None, self.SYNCER_TYPE
            )
            return

        dagruns = sorted(
            dagruns, key=lambda dr: (dr.max_log_id is None, dr.max_log_id)
        )  # type: List[AirflowDagRun]

        bulk_size = self.config.dag_run_bulk_size or len(dagruns)
        for i in range(0, len(dagruns), bulk_size):
            dagruns_chunk = dagruns[i : i + bulk_size]
            dag_run_ids = [dr.id for dr in dagruns_chunk]
            dag_runs_state_data = self.data_fetcher.get_dag_runs_state_data(dag_run_ids)
            logger.info(
                "Updating states for %d dag runs and %d task instances",
                len(dag_runs_state_data.dag_runs),
                len(dag_runs_state_data.task_instances),
            )
            max_logs_ids = [dr.max_log_id for dr in dagruns_chunk if dr.max_log_id]
            self.tracking_service.update_dagruns(
                dag_runs_state_data,
                max(max_logs_ids) if max_logs_ids else None,
                self.SYNCER_TYPE,
            )


def start_runtime_syncer(tracking_source_uid, run=True):
    return start_syncer(
        AirflowRuntimeSyncer, tracking_source_uid=tracking_source_uid, run=run
    )
