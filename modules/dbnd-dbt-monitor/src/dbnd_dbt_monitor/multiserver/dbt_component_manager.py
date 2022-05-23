import logging

from typing import Type

from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorState, DbtServerConfig
from dbnd_dbt_monitor.syncer.dbt_runs_syncer import start_dbt_runs_syncer
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.shared.base_monitor_component_manager import (
    BaseMonitorComponentManager,
)
from airflow_monitor.shared.runners import BaseRunner


logger = logging.getLogger(__name__)

SERVICES_COMPONENTS = {"dbt_runs_syncer": start_dbt_runs_syncer}


class DbtMonitorComponentManager(BaseMonitorComponentManager):
    def __init__(
        self,
        runner: Type[BaseRunner],
        server_config: DbtServerConfig,
        tracking_service: DbndDbtTrackingService,
    ):
        self._plugin_metadata = None
        super(DbtMonitorComponentManager, self).__init__(
            runner, server_config, tracking_service, SERVICES_COMPONENTS
        )

    def _set_running_monitor_state(self, is_monitored_server_alive: bool):
        if not is_monitored_server_alive:
            return

        self.tracking_service.update_monitor_state(
            DbtMonitorState(monitor_status="Running", monitor_error_message=None)
        )

    def _set_starting_monitor_state(self):
        self.tracking_service.update_monitor_state(
            DbtMonitorState(monitor_status="Scheduled", monitor_error_message=None)
        )

    @capture_monitor_exception("checking monitor alive")
    def is_monitored_server_alive(self):
        return True
