import logging

from typing import Type

import airflow_monitor

from airflow_monitor.common import MonitorState, capture_monitor_exception
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    start_runtime_config_updater,
)
from airflow_monitor.data_fetcher import get_data_fetcher
from airflow_monitor.fixer.runtime_fixer import start_runtime_fixer
from airflow_monitor.shared.base_monitor_component_manager import (
    BaseMonitorComponentManager,
)
from airflow_monitor.shared.runners import BaseRunner
from airflow_monitor.syncer.runtime_syncer import start_runtime_syncer
from airflow_monitor.tracking_service import AirflowDbndTrackingService


logger = logging.getLogger(__name__)

SERVICES_COMPONENTS = {
    "state_sync": start_runtime_syncer,
    "fixer": start_runtime_fixer,
    "config_updater": start_runtime_config_updater,
}


class AirflowMonitorComponentManager(BaseMonitorComponentManager):
    def __init__(
        self,
        runner: Type[BaseRunner],
        server_config: AirflowServerConfig,
        tracking_service: AirflowDbndTrackingService,
    ):
        self._plugin_metadata = None
        super(AirflowMonitorComponentManager, self).__init__(
            runner, server_config, tracking_service, SERVICES_COMPONENTS
        )

    def _get_tracking_errors(self):
        from airflow_monitor.validations import (
            get_all_errors,
            get_tracking_validation_steps,
        )

        errors_list = get_all_errors(get_tracking_validation_steps())

        if len(errors_list) == 0:
            return None

        return ", ".join(errors_list)

    def _set_running_monitor_state(self, is_monitored_server_alive: bool):
        if not is_monitored_server_alive or self._plugin_metadata is not None:
            return

        # Check for tracking errors only in the monitor as dag scenario
        error_message = (
            self._get_tracking_errors()
            if self.server_config.fetcher_type == "db"
            else None
        )

        plugin_metadata = get_data_fetcher(self.server_config).get_plugin_metadata()
        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_status="Running",
                airflow_monitor_version=airflow_monitor.__version__ + " v2",
                airflow_version=plugin_metadata.airflow_version,
                airflow_export_version=plugin_metadata.plugin_version,
                airflow_instance_uid=plugin_metadata.airflow_instance_uid,
                monitor_error_message=error_message,
                api_mode=plugin_metadata.api_mode,
            )
        )

        self._plugin_metadata = plugin_metadata

    def _set_starting_monitor_state(self):
        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_status="Scheduled",
                airflow_monitor_version=airflow_monitor.__version__ + " v2",
                monitor_error_message=None,
            )
        )

    @capture_monitor_exception("checking monitor alive")
    def is_monitored_server_alive(self):
        return get_data_fetcher(self.server_config).is_alive()
