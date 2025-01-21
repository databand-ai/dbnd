# © Copyright Databand.ai, an IBM Company 2022
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Optional

from dbnd._core.utils.timezone import utcnow
from dbnd_airflow.export_plugin.models import DagRunState
from dbnd_monitor.adapter import AssetState, AssetToState
from dbnd_monitor.base_tracking_service import BaseTrackingService
from dbnd_monitor.error_handling.error_aggregator import ErrorAggregatorResult
from dbnd_monitor.integration_management_service import IntegrationManagementService
from dbnd_monitor.reporting_service import ReportingService
from test_dbnd_airflow_monitor.airflow_utils import can_be_dead
from test_dbnd_airflow_monitor.mocks.mock_airflow_data_fetcher import MockDagRun


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
    def __init__(self, tracking_source_uid=None):
        super(MockTrackingService, self).__init__(
            monitor_type="airflow", tracking_source_uid=tracking_source_uid
        )

        self.dag_runs = []  # type: List[MockDagRun]

        self.last_seen_dag_run_id = None
        self.alive = True

        self.airflow_monitor_version = "2.0"

    @can_be_dead
    @ticking
    def get_active_assets(self, integration_id: str, syncer_instance_id: str):
        dag_run_ids = [
            dag_run.id
            for dag_run in self.dag_runs
            if dag_run.state == DagRunState.RUNNING and not dag_run.is_paused
        ]
        asset_ids = list(map(str, dag_run_ids))
        return list(map(lambda id: AssetToState(id, AssetState.ACTIVE), asset_ids))

    @can_be_dead
    @ticking
    def get_syncer_info(self):
        return {}

    @can_be_dead
    @ticking
    def get_last_cursor(self, integration_id: str, syncer_instance_id: str):
        return self.last_seen_dag_run_id

    @can_be_dead
    @ticking
    def update_last_cursor(
        self, integration_id: str, syncer_instance_id: str, state: str, data: int
    ):
        self.last_seen_dag_run_id = data


class MockIntegrationManagementService(IntegrationManagementService):
    def __init__(self):
        super(MockIntegrationManagementService, self).__init__()
        self.alive = True
        self.mock_configs = []
        self.monitor_state_updates = defaultdict(list)

    @can_be_dead
    @ticking
    def get_all_integration_configs(
        self,
        monitor_type: str,
        syncer_name: Optional[str] = None,
        source_instance_uid: Optional[str] = None,
    ) -> List[Dict]:
        return self.mock_configs


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
