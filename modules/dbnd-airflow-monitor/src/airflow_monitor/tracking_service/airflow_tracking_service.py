# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from dbnd_monitor.adapter.adapter import AssetToState
from dbnd_monitor.base_tracking_service import BaseTrackingService


logger = logging.getLogger(__name__)

SYNCER_INSTANCE_ID_CURSOR = "airflow_runs_syncer"
AIRFLOW_MONITOR_CURSOR_NAME = "last_seen_dag_run_id"
DAG_RUN_ASSET_TYPE = "dag_run"


class AirflowTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        super(AirflowTrackingService, self).__init__(
            monitor_type=monitor_type, tracking_source_uid=tracking_source_uid
        )
        self.airflow_instance_uid = DbFetcher.get_airflow_instance_uid()

    def get_syncer_info(self):
        params = {"airflow_instance_uid": self.airflow_instance_uid}
        endpoint = f"tracking-monitor/{self.tracking_source_uid}/server_info"

        return self._api_client.api_request(
            endpoint=endpoint, method="GET", data=None, query=params
        )

    def update_last_cursor(
        self,
        integration_id: str,
        syncer_instance_id: str,
        state: str,
        data: int,
        cursor_name: str = AIRFLOW_MONITOR_CURSOR_NAME,
    ):
        # For Airflow, cursor name is not 'last_cursor_value' but 'last_seen_dag_run_id'
        super().update_last_cursor(
            integration_id, SYNCER_INSTANCE_ID_CURSOR, state, data, cursor_name
        )

    def get_last_cursor(
        self,
        integration_id: str,
        syncer_instance_id: str,
        cursor_name: str = AIRFLOW_MONITOR_CURSOR_NAME,
    ) -> int:
        # For Airflow, cursor name is not 'last_cursor_value' but 'last_seen_dag_run_id'
        return super().get_last_cursor(
            integration_id, SYNCER_INSTANCE_ID_CURSOR, cursor_name
        )

    def get_active_assets(
        self,
        integration_id: str,
        syncer_instance_id: str,
        asset_type: str = DAG_RUN_ASSET_TYPE,
    ) -> List[AssetToState]:
        # For Airflow, assets name is not 'run' but 'dag_run'
        return super().get_active_assets(integration_id, syncer_instance_id, asset_type)

    def save_assets_state(
        self,
        integration_id: str,
        syncer_instance_id: str,
        assets_to_state: List[AssetToState],
        asset_type: str = DAG_RUN_ASSET_TYPE,
    ):
        # For Airflow, assets name is not 'run' but 'dag_run'
        super().save_assets_state(
            integration_id, syncer_instance_id, assets_to_state, asset_type
        )
