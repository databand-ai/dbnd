import logging

from typing import List, Optional

from airflow_monitor.airflow_monitor_utils import log_received_tasks, send_metrics
from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.config_data import AirflowServerConfig


logger = logging.getLogger(__name__)


class AirflowDataFetcher(object):
    def __init__(self, config: AirflowServerConfig):
        self.source_name = config.source_name

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

    def get_plugin_metadata(self) -> PluginMetadata:
        return PluginMetadata()

    def _on_data_received(self, json_data, data_source):
        log_received_tasks(data_source, json_data)
        send_metrics(self.source_name, json_data.get("airflow_export_meta"))
