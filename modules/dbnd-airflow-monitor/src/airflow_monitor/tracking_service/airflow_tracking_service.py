# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Optional

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from dbnd_airflow.utils import get_or_create_airflow_instance_uid
from dbnd_monitor.base_tracking_service import BaseTrackingService


logger = logging.getLogger(__name__)


class AirflowTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        super(AirflowTrackingService, self).__init__(
            monitor_type=monitor_type, tracking_source_uid=tracking_source_uid
        )
        self.airflow_instance_uid = get_or_create_airflow_instance_uid()

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
        data = last_seen_values.as_dict()
        data["airflow_instance_uid"] = self.airflow_instance_uid
        self._make_request("update_last_seen_values", method="POST", data=data)

    def get_active_dag_runs(
        self, dag_ids: Optional[str] = None, excluded_dag_ids: Optional[str] = None
    ) -> DbndDagRunsResponse:
        params = {}
        if dag_ids:
            params["dag_ids"] = dag_ids
        if excluded_dag_ids:
            params["excluded_dag_ids"] = excluded_dag_ids
        params["airflow_instance_uid"] = self.airflow_instance_uid
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
        data["airflow_instance_uid"] = self.airflow_instance_uid

        return self.save_tracking_data(data)

    def update_dagruns(self, dag_runs_state_data: DagRunsStateData, syncer_type: str):
        data = dag_runs_state_data.as_dict()
        data["syncer_type"] = syncer_type
        data["airflow_instance_uid"] = self.airflow_instance_uid
        return self.save_tracking_data(data)

    def get_syncer_info(self):
        params = {"airflow_instance_uid": self.airflow_instance_uid}
        return self._make_request("server_info", method="GET", data=None, query=params)
