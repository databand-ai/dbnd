from copy import copy
from functools import wraps
from typing import List, Optional

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowServerConfig, MonitorConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.tracking_service.af_tracking_service import (
    DbndAirflowTrackingService,
    ServersConfigurationService,
)
from test_dbnd_airflow_monitor.airflow_utils import can_be_dead
from test_dbnd_airflow_monitor.mock_airflow_data_fetcher import MockDagRun


class Ticker:
    def __init__(self):
        self.point_in_time = 0

    def tick(self):
        self.point_in_time += 1
        return self.point_in_time

    @property
    def now(self):
        return self.point_in_time

    def reset(self):
        self.point_in_time = 0


ticker = Ticker()


def ticking(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        finally:
            ticker.tick()

    return wrapped


class MockServersConfigService(ServersConfigurationService):
    def __init__(self):
        self.alive = True
        self.mock_servers = []  # type: List[AirflowServerConfig]

    @can_be_dead
    @ticking
    def get_all_servers_configuration(self) -> List[AirflowServerConfig]:
        return self.mock_servers


class MockTrackingService(DbndAirflowTrackingService):
    def __init__(self, tracking_source_uid=None):
        super(MockTrackingService, self).__init__(tracking_source_uid)
        self.dag_runs = []  # type: List[MockDagRun]

        self.config = MonitorConfig(tracking_source_uid=tracking_source_uid)
        self.last_seen_dag_run_id = None
        self.last_seen_log_id = None
        self.alive = True

    @can_be_dead
    @ticking
    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        if self.last_seen_log_id is None:
            self.last_seen_log_id = last_seen_values.last_seen_log_id
        if self.last_seen_dag_run_id is None:
            self.last_seen_dag_run_id = last_seen_values.last_seen_dag_run_id

    @can_be_dead
    @ticking
    def get_all_dag_runs(self, start_time_window: int) -> DbndDagRunsResponse:
        return DbndDagRunsResponse(
            dag_run_ids=[dag_run.id for dag_run in self.dag_runs],
            last_seen_dag_run_id=None,
            last_seen_log_id=None,
        )

    @can_be_dead
    @ticking
    def get_active_dag_runs(self, start_time_window: int) -> DbndDagRunsResponse:
        return DbndDagRunsResponse(
            dag_run_ids=[
                dag_run.id
                for dag_run in self.dag_runs
                if dag_run.state == "RUNNING" and not dag_run.is_paused
            ],
            last_seen_dag_run_id=self.last_seen_dag_run_id,
            last_seen_log_id=self.last_seen_log_id,
        )

    def _get_dagrun_index(self, dr_or_ti):
        for i, dr in enumerate(self.dag_runs):
            if (
                dr.dag_id == dr_or_ti.dag_id
                and dr.execution_date == dr_or_ti.execution_date
            ):
                return i

    @can_be_dead
    @ticking
    def init_dagruns(
        self,
        dag_runs_full_data: DagRunsFullData,
        last_seen_dag_run_id: int,
        syncer_type: str,
    ):
        for dag_run in dag_runs_full_data.dag_runs:
            dag_run = copy(dag_run)  # type: MockDagRun
            dag_run.test_created_at = ticker.now
            dag_run.test_updated_at = ticker.now

            i = self._get_dagrun_index(dag_run)
            if i is not None:
                self.dag_runs.pop(i)
            self.dag_runs.append(dag_run)

        if last_seen_dag_run_id > self.last_seen_dag_run_id:
            self.last_seen_dag_run_id = last_seen_dag_run_id

    @can_be_dead
    @ticking
    def update_dagruns(
        self,
        dag_runs_state_data: DagRunsStateData,
        last_seen_log_id: int,
        syncer_type: str,
    ):
        for ti in dag_runs_state_data.task_instances:
            self.dag_runs[self._get_dagrun_index(ti)].test_updated_at = ticker.now
        for dr in dag_runs_state_data.dag_runs:  # type: MockDagRun
            inner_dr = self.dag_runs[self._get_dagrun_index(dr)]
            inner_dr.state = dr.state
            inner_dr.is_paused = dr.is_paused

        if last_seen_log_id and last_seen_log_id > self.last_seen_log_id:
            self.last_seen_log_id = last_seen_log_id

    @can_be_dead
    @ticking
    def get_monitor_configuration(self) -> MonitorConfig:
        return self.config
