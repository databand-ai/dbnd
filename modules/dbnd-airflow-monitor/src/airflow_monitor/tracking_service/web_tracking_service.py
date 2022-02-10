import logging

from datetime import datetime, timedelta
from typing import Type

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.shared.base_server_monitor_config import TrackingServiceConfig
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from dbnd._core.utils.timezone import utctoday
from dbnd.api.serialization.tracking import UpdateAirflowMonitorStateRequestSchema


LONG_REQUEST_TIMEOUT = 300

logger = logging.getLogger(__name__)


def _min_start_time(start_time_window: int) -> datetime:
    if not start_time_window:
        return None

    return utctoday() - timedelta(days=start_time_window)


class AirflowDbndTrackingService(BaseDbndTrackingService):
    def __init__(
        self,
        monitor_type: str,
        tracking_source_uid: str,
        tracking_service_config: TrackingServiceConfig,
        server_monitor_config: Type[AirflowServerConfig],
    ):
        super(AirflowDbndTrackingService, self).__init__(
            monitor_type=monitor_type,
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=tracking_service_config,
            server_monitor_config=server_monitor_config,
            monitor_state_schema=UpdateAirflowMonitorStateRequestSchema,
        )

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        self._make_request(
            "update_last_seen_values", method="POST", data=last_seen_values.as_dict()
        )

    def get_all_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        params = {}
        start_time = _min_start_time(start_time_window)
        if start_time:
            params["min_start_time"] = start_time.isoformat()
        if dag_ids:
            params["dag_ids"] = dag_ids

        response = self._make_request(
            "get_all_dag_runs", method="GET", data=None, query=params
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def get_active_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        params = {}
        start_time = _min_start_time(start_time_window)
        if start_time:
            params["min_start_time"] = start_time.isoformat()
        if dag_ids:
            params["dag_ids"] = dag_ids

        response = self._make_request(
            "get_running_dag_runs", method="GET", data=None, query=params
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def init_dagruns(
        self,
        dag_runs_full_data: DagRunsFullData,
        last_seen_dag_run_id: int,
        syncer_type: str,
    ):
        data = dag_runs_full_data.as_dict()
        data["last_seen_dag_run_id"] = last_seen_dag_run_id
        data["syncer_type"] = syncer_type
        response = self._make_request(
            "init_dagruns",
            method="POST",
            data=data,
            request_timeout=LONG_REQUEST_TIMEOUT,
        )
        return response

    def update_dagruns(
        self,
        dag_runs_state_data: DagRunsStateData,
        last_seen_log_id: int,
        syncer_type: str,
    ):
        data = dag_runs_state_data.as_dict()
        data["last_seen_log_id"] = last_seen_log_id
        data["syncer_type"] = syncer_type
        response = self._make_request(
            "update_dagruns",
            method="POST",
            data=data,
            request_timeout=LONG_REQUEST_TIMEOUT,
        )
        return response

    def get_syncer_info(self):
        return self._make_request("server_info", method="GET", data={})
