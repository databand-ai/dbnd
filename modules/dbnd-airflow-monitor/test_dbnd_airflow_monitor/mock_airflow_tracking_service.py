from copy import copy
from functools import wraps
from typing import List

import attr

from airflow_monitor.common import MonitorState
from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.shared import get_tracking_service_config_from_dbnd
from airflow_monitor.shared.base_tracking_service import (
    BaseDbndTrackingService,
    WebServersConfigurationService,
)
from dbnd.api.serialization.tracking import UpdateAirflowMonitorStateRequestSchema
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


class MockServersConfigService(WebServersConfigurationService):
    def __init__(self):

        super(MockServersConfigService, self).__init__(
            monitor_type="airflow",
            tracking_service_config=get_tracking_service_config_from_dbnd(),
            server_monitor_config=AirflowServerConfig,
        )
        self.alive = True
        self.mock_servers = []  # type: List[AirflowServerConfig]

    @can_be_dead
    @ticking
    def get_all_servers_configuration(
        self, monitor_config=None
    ) -> List[AirflowServerConfig]:
        return self.mock_servers


class MockTrackingService(BaseDbndTrackingService):
    def __init__(self, tracking_source_uid=None):
        super(MockTrackingService, self).__init__(
            monitor_type="airflow",
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=get_tracking_service_config_from_dbnd(),
            server_monitor_config=AirflowServerConfig,
            monitor_state_schema=UpdateAirflowMonitorStateRequestSchema,
        )

        self.dag_runs = []  # type: List[MockDagRun]

        self.config = AirflowServerConfig(
            source_name="test",
            source_type="airflow",
            tracking_source_uid=tracking_source_uid,
            interval=0,
            sync_interval=0,
            fix_interval=0,
            config_updater_enabled=False,
            config_updater_interval=0,
        )
        self.last_seen_dag_run_id = None
        self.last_seen_log_id = None
        self.alive = True

        self.monitor_state_updates = []

    @can_be_dead
    @ticking
    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        last_seen_values = LastSeenValues.from_dict(last_seen_values.as_dict())
        if self.last_seen_log_id is None:
            self.last_seen_log_id = last_seen_values.last_seen_log_id
        if self.last_seen_dag_run_id is None:
            self.last_seen_dag_run_id = last_seen_values.last_seen_dag_run_id

    @can_be_dead
    @ticking
    def get_all_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        dag_ids_list = dag_ids.split(",") if dag_ids else None

        return DbndDagRunsResponse(
            dag_run_ids=[
                dag_run.id
                for dag_run in self.dag_runs
                if dag_ids_list is None or dag_run.dag_id in dag_ids_list
            ],
            last_seen_dag_run_id=None,
            last_seen_log_id=None,
        )

    @can_be_dead
    @ticking
    def get_active_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        dag_ids_list = dag_ids.split(",") if dag_ids else None

        return DbndDagRunsResponse(
            dag_run_ids=[
                dag_run.id
                for dag_run in self.dag_runs
                if dag_run.state == "RUNNING"
                and not dag_run.is_paused
                and (dag_ids_list is None or dag_run.dag_id in dag_ids_list)
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
        dag_runs_full_data = DagRunsFullData.from_dict(dag_runs_full_data.as_dict())
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
        dag_runs_state_data = DagRunsStateData.from_dict(dag_runs_state_data.as_dict())
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
    def get_monitor_configuration(self) -> AirflowServerConfig:
        return self.config

    @can_be_dead
    @ticking
    def update_monitor_state(self, monitor_state: MonitorState):
        self.monitor_state_updates.append(monitor_state.as_dict())

    @can_be_dead
    @ticking
    def get_syncer_info(self):
        return {}

    @property
    def current_monitor_state(self):
        monitor_state = {k: None for k in attr.fields_dict(MonitorState).keys()}
        for s in self.monitor_state_updates:
            monitor_state.update(s)
        return MonitorState(**monitor_state)
