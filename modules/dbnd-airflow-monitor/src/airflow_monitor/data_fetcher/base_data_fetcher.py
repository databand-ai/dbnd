from typing import List, Optional

from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowServerConfig


class AirflowDataFetcher(object):
    def __init__(self, config):
        # type: (AirflowServerConfig) -> None
        pass

    def get_last_seen_values(self) -> LastSeenValues:
        raise NotImplementedError()

    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int],
        last_seen_log_id: Optional[int],
        extra_dag_run_ids: Optional[List[int]],
        dag_ids: Optional[str],
    ) -> AirflowDagRunsResponse:
        raise NotImplementedError()

    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        raise NotImplementedError()

    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()
