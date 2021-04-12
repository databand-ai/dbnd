from typing import List

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    MonitorState,
)
from airflow_monitor.common.config_data import AirflowServerConfig, MonitorConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse


class DbndAirflowTrackingService(object):
    def __init__(self, tracking_source_uid):
        self.tracking_source_uid = tracking_source_uid

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        raise NotImplementedError()

    def get_all_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        raise NotImplementedError()

    def get_active_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        raise NotImplementedError()

    def init_dagruns(
        self,
        dag_runs_full_data: DagRunsFullData,
        last_seen_dag_run_id: int,
        syncer_type: str,
    ):
        raise NotImplementedError()

    def update_dagruns(
        self,
        dag_runs_state_data: DagRunsStateData,
        last_seen_log_id: int,
        syncer_type: str,
    ):
        raise NotImplementedError()

    def get_monitor_configuration(self) -> MonitorConfig:
        raise NotImplementedError()

    def update_monitor_state(self, monitor_state: MonitorState):
        raise NotImplementedError()


class ServersConfigurationService(object):
    def get_all_servers_configuration(self) -> List[AirflowServerConfig]:
        raise NotImplementedError()
