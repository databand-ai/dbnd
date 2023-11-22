# © Copyright Databand.ai, an IBM Company 2022
from collections import defaultdict
from copy import copy
from functools import wraps
from typing import List

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.shared.base_integration import BaseIntegration
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.error_aggregator import ErrorAggregatorResult
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from airflow_monitor.shared.reporting_service import ReportingService
from dbnd._core.utils.timezone import utcnow
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


class MockTrackingService(BaseTrackingService):
    def __init__(self, server_id=None):
        super(MockTrackingService, self).__init__(
            monitor_type="airflow", server_id=server_id
        )

        self.dag_runs = []  # type: List[MockDagRun]

        self.last_seen_dag_run_id = None
        self.last_seen_log_id = None
        self.alive = True

        self.airflow_monitor_version = "2.0"

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
        plugin_meta_data: PluginMetadata,
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
    def get_syncer_info(self):
        return {}


class MockIntegrationManagementService(IntegrationManagementService):
    def __init__(self):
        super(MockIntegrationManagementService, self).__init__([])
        self.alive = True
        self.mock_integrations = []  # type: List[BaseIntegration]
        self.monitor_state_updates = defaultdict(list)

    @can_be_dead
    @ticking
    def get_all_integrations(self, monitor_config=None) -> List[BaseIntegration]:
        return self.mock_integrations


class MockReportingService(ReportingService):
    def __init__(self, monitor_type):
        super().__init__(monitor_type)
        self.alive = True
        self.metadata = None
        self.error = None
        self.last_sync_time = None
        self.last_update_time = None

    def report_monitor_time_data(self, integration_uid, synced_new_data=False):
        current_time = utcnow()
        self.last_sync_time = current_time
        if synced_new_data:
            self.last_update_time = current_time

    def report_metadata(self, integration_uid, metadata):
        self.metadata = metadata

    def _report_error(self, integration_uid, res: ErrorAggregatorResult):
        if res.should_update:
            self.error = res.message
