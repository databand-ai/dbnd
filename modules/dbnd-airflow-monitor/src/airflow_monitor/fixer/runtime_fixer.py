import logging

from typing import List

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.base_component import BaseMonitorComponent, start_syncer


logger = logging.getLogger(__name__)


class AirflowRuntimeFixer(BaseMonitorComponent):
    SYNCER_TYPE = "runtime_fixer"

    def __init__(self, *args, **kwargs):
        super(AirflowRuntimeFixer, self).__init__(*args, **kwargs)
        self.sleep_interval = self.config.fix_interval

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        dbnd_response = self.tracking_service.get_all_dag_runs(
            start_time_window=self.config.start_time_window,
            dag_ids=self.config.dag_ids,
        )
        if not dbnd_response.dag_run_ids:
            logger.debug("last_seen_dag_run_id is undefined, nothing to do")
            return

        self.update_dagruns(dbnd_response.dag_run_ids)

    @capture_monitor_exception
    def update_dagruns(self, dag_run_ids: List[int]):
        if not dag_run_ids:
            return

        dag_run_ids = sorted(dag_run_ids, reverse=True)
        bulk_size = self.config.dag_run_bulk_size or len(dag_run_ids)
        for i in range(0, len(dag_run_ids), bulk_size):
            dag_run_ids_chunk = dag_run_ids[i : i + bulk_size]
            dag_runs_state_data = self.data_fetcher.get_dag_runs_state_data(
                dag_run_ids_chunk
            )
            self.tracking_service.update_dagruns(
                dag_runs_state_data, None, self.SYNCER_TYPE
            )


def start_runtime_fixer(tracking_source_uid, run=True):
    return start_syncer(
        AirflowRuntimeFixer, tracking_source_uid=tracking_source_uid, run=run
    )
