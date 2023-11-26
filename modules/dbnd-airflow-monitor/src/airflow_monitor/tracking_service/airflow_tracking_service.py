# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import datetime, timedelta
from typing import Optional

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from dbnd._core.utils.timezone import utctoday


logger = logging.getLogger(__name__)


def _min_start_time(start_time_window: int) -> Optional[datetime]:
    if not start_time_window:
        return None

    return utctoday() - timedelta(days=start_time_window)


class AirflowTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        super(AirflowTrackingService, self).__init__(
            monitor_type=monitor_type, tracking_source_uid=tracking_source_uid
        )

    def _generate_url_for_tracking_service(self, name: str) -> str:
        return f"tracking-monitor/{self.tracking_source_uid}/{name}"

    def _make_request(
        self,
        name: str,
        method: str,
        data: dict,
        query: dict = None,
        request_timeout: int = None,
    ) -> dict:
        return self._api_client.api_request(
            endpoint=self._generate_url_for_tracking_service(name),
            method=method,
            data=data,
            query=query,
            request_timeout=request_timeout,
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
        plugin_meta_data: PluginMetadata,
    ):
        data = dag_runs_full_data.as_dict()
        data["last_seen_dag_run_id"] = last_seen_dag_run_id
        data["syncer_type"] = syncer_type
        data["airflow_export_meta"] = plugin_meta_data.as_dict()

        return self.save_tracking_data(data)

    def update_dagruns(
        self,
        dag_runs_state_data: DagRunsStateData,
        last_seen_log_id: int,
        syncer_type: str,
    ):
        data = dag_runs_state_data.as_dict()
        data["last_seen_log_id"] = last_seen_log_id
        data["syncer_type"] = syncer_type
        return self.save_tracking_data(data)

    def get_syncer_info(self):
        return self._make_request("server_info", method="GET", data={})
