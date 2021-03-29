from typing import List

from airflow_monitor.common import (
    AirflowServerConfig,
    DagRunsFullData,
    DagRunsStateData,
    DbndDagRunsResponse,
    LastSeenValues,
    MonitorConfig,
)


class DbndAirflowTrackingService(object):
    def __init__(self, tracking_source_uid):
        self.tracking_source_uid = tracking_source_uid

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        raise NotImplementedError()

    def get_dbnd_dags_to_sync(
        self, max_execution_date_window: int
    ) -> DbndDagRunsResponse:
        raise NotImplementedError()

    def init_dagruns(
        self, dag_runs_full_data: DagRunsFullData, last_seen_dag_run_id: int
    ):
        raise NotImplementedError()

    def update_dagruns(
        self, dag_runs_state_data: DagRunsStateData, last_seen_log_id: int
    ):
        raise NotImplementedError()

    def get_monitor_configuration(self) -> MonitorConfig:
        raise NotImplementedError()


class ServersConfigurationService(object):
    def get_all_servers_configuration(self) -> List[AirflowServerConfig]:
        raise NotImplementedError()
