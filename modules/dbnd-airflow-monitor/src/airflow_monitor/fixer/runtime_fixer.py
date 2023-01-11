# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)


logger = logging.getLogger(__name__)


class AirflowRuntimeFixer(BaseComponent):
    SYNCER_TYPE = "runtime_fixer"

    tracking_service: AirflowTrackingService
    config: AirflowServerConfig
    data_fetcher: AirflowDataFetcher

    @property
    def sleep_interval(self):
        return self.config.fix_interval

    def _sync_once(self):
        dbnd_response = self.tracking_service.get_all_dag_runs(
            start_time_window=self.config.start_time_window, dag_ids=self.config.dag_ids
        )
        if not dbnd_response.dag_run_ids:
            logger.debug("last_seen_dag_run_id is undefined, nothing to do")
            return

        self.update_runs(dbnd_response.dag_run_ids)

    def update_runs(self, run_ids_to_update: List[int]):
        if not run_ids_to_update:
            return

        dag_run_ids = sorted(run_ids_to_update, reverse=True)
        bulk_size = self.config.dag_run_bulk_size or len(dag_run_ids)
        for i in range(0, len(dag_run_ids), bulk_size):
            dag_run_ids_chunk = dag_run_ids[i : i + bulk_size]
            dag_runs_state_data = self.data_fetcher.get_dag_runs_state_data(
                dag_run_ids_chunk
            )
            self.tracking_service.update_dagruns(
                dag_runs_state_data, None, self.SYNCER_TYPE
            )
