import logging

from typing import List

from airflow_monitor.common import AirflowDagRun, AirflowDagRunsResponse
from airflow_monitor.syncer.base_syncer import BaseAirflowSyncer, start_syncer


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


class AirflowRuntimeSyncer(BaseAirflowSyncer):
    def sync_once(self):
        dbnd_response = self.tracking_service.get_dbnd_dags_to_sync(
            max_execution_date_window=self.config.max_execution_date_window
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

            dbnd_response = self.tracking_service.get_dbnd_dags_to_sync(
                max_execution_date_window=self.config.max_execution_date_window
            )

        airflow_response = self.data_fetcher.get_airflow_dagruns_to_sync(
            last_seen_dag_run_id=dbnd_response.last_seen_dag_run_id,
            last_seen_log_id=dbnd_response.last_seen_log_id,
            extra_dag_run_ids=dbnd_response.dag_run_ids,
        )  # type: AirflowDagRunsResponse

        dagruns_to_init, dagruns_to_update = categorize_dag_runs(
            airflow_response.dag_runs, dbnd_response.dag_run_ids
        )

        self.init_dagruns(dagruns_to_init)
        self.update_dagruns(dagruns_to_update)

    def init_dagruns(self, dagruns: List[AirflowDagRun]):
        if not dagruns:
            return

        dagruns = sorted(dagruns, key=lambda dr: dr.id)  # type: List[AirflowDagRun]

        bulk_size = self.config.init_dag_run_bulk_size or len(dagruns)
        for i in range(0, len(dagruns), bulk_size):
            dagruns_chunk = dagruns[i : i + bulk_size]
            dag_runs_full_data = self.data_fetcher.get_full_dag_runs(dagruns_chunk)
            self.tracking_service.init_dagruns(
                dag_runs_full_data, max(dr.id for dr in dagruns_chunk)
            )

    def update_dagruns(self, dagruns: List[AirflowDagRun]):
        if not dagruns:
            return

        dagruns = sorted(
            dagruns, key=lambda dr: (dr.max_log_id is None, dr.max_log_id)
        )  # type: List[AirflowDagRun]

        bulk_size = self.config.init_dag_run_bulk_size or len(dagruns)
        for i in range(0, len(dagruns), bulk_size):
            dagruns_chunk = dagruns[i : i + bulk_size]
            dag_runs_state_data = self.data_fetcher.get_dag_runs_state_data(
                dagruns_chunk
            )
            max_logs_ids = [dr.max_log_id for dr in dagruns_chunk if dr.max_log_id]
            self.tracking_service.update_dagruns(
                dag_runs_state_data, max(max_logs_ids) if max_logs_ids else None
            )


def start_runtime_syncer(tracking_source_uid, run=True):
    return start_syncer(
        AirflowRuntimeSyncer, tracking_source_uid=tracking_source_uid, run=run
    )
