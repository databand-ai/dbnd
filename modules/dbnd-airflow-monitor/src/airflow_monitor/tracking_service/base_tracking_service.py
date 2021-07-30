from typing import List

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    MonitorState,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.tracking_service.error_aggregator import ErrorAggregator


class DbndAirflowTrackingService(object):
    def __init__(self, tracking_source_uid):
        self.tracking_source_uid = tracking_source_uid
        self.error_aggregator = ErrorAggregator()

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

    def get_airflow_server_configuration(self) -> AirflowServerConfig:
        raise NotImplementedError()

    def update_monitor_state(self, monitor_state: MonitorState):
        raise NotImplementedError()

    def report_error(self, reporting_obj_ref, err_message):
        res = self.error_aggregator.report(reporting_obj_ref, err_message)
        if res.should_update:
            self.update_monitor_state(MonitorState(monitor_error_message=res.message))


class ServersConfigurationService(object):
    def get_all_servers_configuration(
        self, airflow_config: AirflowMonitorConfig
    ) -> List[AirflowServerConfig]:
        raise NotImplementedError()

    def send_prometheus_metrics(self, job_name: str, full_metrics: str):
        raise NotImplementedError()
