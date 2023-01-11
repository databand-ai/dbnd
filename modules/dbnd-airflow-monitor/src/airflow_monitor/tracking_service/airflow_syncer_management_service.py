# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List, Optional, Type

import airflow_monitor

from airflow_monitor.common.airflow_data import MonitorState, PluginMetadata
from airflow_monitor.common.metric_reporter import generate_latest_metrics
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)


logger = logging.getLogger(__name__)


def _get_tracking_errors():
    from airflow_monitor.validations import (
        get_all_errors,
        get_tracking_validation_steps,
    )

    errors_list = get_all_errors(get_tracking_validation_steps())

    if len(errors_list) == 0:
        return None

    return ", ".join(errors_list)


class AirflowSyncerManagementService(BaseSyncerManagementService):
    def __init__(
        self,
        monitor_type: str,
        server_monitor_config: Type[BaseServerConfig],
        plugin_metadata: PluginMetadata,
    ):
        super(AirflowSyncerManagementService, self).__init__(
            monitor_type, server_monitor_config
        )
        self.airflow_monitor_version = airflow_monitor.__version__ + " v2"
        self.plugin_metadata = plugin_metadata

    def get_all_servers_configuration(
        self, monitor_config: Optional[AirflowMonitorConfig] = None
    ) -> List[BaseServerConfig]:
        servers = super(
            AirflowSyncerManagementService, self
        ).get_all_servers_configuration(monitor_config)

        if not servers:
            return servers

        if not monitor_config.syncer_name:
            return [s for s in servers if s.fetcher_type != "db"]

        filtered_servers = [
            s for s in servers if s.source_name == monitor_config.syncer_name
        ]
        if not filtered_servers:
            available_servers = ",".join(
                [s.source_name for s in servers if s.source_name]
            )
            logger.error(
                "No syncer configuration found matching name '%s'. Available syncers: %s Please provide correct syncer name (using --syncer-name parameter, env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
                monitor_config.syncer_name,
                available_servers,
            )
        return filtered_servers

    def send_metrics(self, monitor_config):
        metrics = generate_latest_metrics().decode("utf-8")
        self.send_prometheus_metrics(metrics, monitor_config.syncer_name)

    def update_monitor_state(self, server_id, monitor_state: MonitorState):
        data = monitor_state.as_dict()

        self._api_client.api_request(
            endpoint=f"source_monitor/{self.monitor_type}/{server_id}/state",
            method="POST",
            data=data,
        )

    def set_starting_monitor_state(self, server_id):
        self.update_monitor_state(
            server_id,
            MonitorState(
                monitor_status="Scheduled",
                airflow_monitor_version=self.airflow_monitor_version,
                monitor_error_message=None,
            ),
        )

    def set_running_monitor_state(self, server_id):
        error_message = _get_tracking_errors()

        self.update_monitor_state(
            server_id,
            MonitorState(
                monitor_status="Running",
                airflow_monitor_version=self.airflow_monitor_version,
                airflow_version=self.plugin_metadata.airflow_version,
                airflow_export_version=self.plugin_metadata.plugin_version,
                airflow_instance_uid=self.plugin_metadata.airflow_instance_uid,
                monitor_error_message=error_message,
                api_mode=self.plugin_metadata.api_mode,
            ),
        )
