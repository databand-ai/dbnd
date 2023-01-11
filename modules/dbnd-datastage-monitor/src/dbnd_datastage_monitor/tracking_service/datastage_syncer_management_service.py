# © Copyright Databand.ai, an IBM Company 2022

from dbnd_datastage_monitor.data.datastage_config_data import DataStageMonitorState

from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)
from dbnd._core.utils.timezone import utcnow


class DataStageSyncersManagementService(BaseSyncerManagementService):
    def _get_monitor_config_data(self):
        result = self._api_client.api_request(
            endpoint="datastage_syncers", method="GET", data=None
        )
        return result

    def update_monitor_state(self, server_id, monitor_state: DataStageMonitorState):
        data = monitor_state.as_dict()
        self._api_client.api_request(
            endpoint=f"datastage_syncers/{server_id}/state", method="PATCH", data=data
        )

    def update_last_sync_time(self, server_id):
        self.update_monitor_state(
            server_id, DataStageMonitorState(last_sync_time=utcnow())
        )

    def set_running_monitor_state(self, server_id):
        self.update_monitor_state(
            server_id,
            DataStageMonitorState(monitor_status="Running", monitor_error_message=None),
        )

    def set_starting_monitor_state(self, server_id):
        self.update_monitor_state(
            server_id,
            DataStageMonitorState(
                monitor_status="Scheduled", monitor_error_message=None
            ),
        )
