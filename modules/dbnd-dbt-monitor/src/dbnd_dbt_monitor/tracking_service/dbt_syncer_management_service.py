from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorState

# © Copyright Databand.ai, an IBM Company 2022
from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)


class DbtSyncersManagementService(BaseSyncerManagementService):
    def _get_monitor_config_data(self):
        result = self._api_client.api_request(
            endpoint="dbt_syncers", method="GET", data=None
        )
        return result

    def update_monitor_state(self, server_id, monitor_state: DbtMonitorState):
        data = monitor_state.as_dict()
        self._api_client.api_request(
            endpoint=f"dbt_syncers/{server_id}/state", method="PATCH", data=data
        )

    def set_running_monitor_state(self, server_id):
        self.update_monitor_state(
            server_id,
            DbtMonitorState(monitor_status="Running", monitor_error_message=None),
        )

    def set_starting_monitor_state(self, server_id):
        self.update_monitor_state(
            server_id,
            DbtMonitorState(monitor_status="Scheduled", monitor_error_message=None),
        )
