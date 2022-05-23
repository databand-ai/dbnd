import logging

from typing import Type
from uuid import UUID

from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorConfig
from dbnd_dbt_monitor.multiserver.dbt_component_manager import (
    DbtMonitorComponentManager,
)
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)
from dbnd_dbt_monitor.tracking_service.tracking_service_decorators import (
    get_servers_configuration_service,
    get_tracking_service,
)

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.shared.base_multiserver import BaseMultiServerMonitor
from airflow_monitor.shared.base_tracking_service import WebServersConfigurationService
from airflow_monitor.shared.runners import RUNNER_FACTORY, BaseRunner


logger = logging.getLogger(__name__)


class DbtMultiServerMonitor(BaseMultiServerMonitor):
    runner: Type[BaseRunner]
    monitor_component_manager: Type[DbtMonitorComponentManager]
    servers_configuration_service: WebServersConfigurationService
    monitor_config: DbtMonitorConfig

    def __init__(
        self,
        runner: Type[BaseRunner],
        monitor_component_manager: Type[DbtMonitorComponentManager],
        servers_configuration_service: WebServersConfigurationService,
        monitor_config: DbtMonitorConfig,
    ):
        super(DbtMultiServerMonitor, self).__init__(
            runner,
            monitor_component_manager,
            servers_configuration_service,
            monitor_config,
        )

    @capture_monitor_exception
    def _send_metrics(self):
        pass

    def _assert_valid_config(self):
        pass

    def _get_tracking_service(
        self, tracking_source_uid: UUID
    ) -> DbndDbtTrackingService:
        return get_tracking_service(tracking_source_uid)

    def _filter_servers(self, servers):
        filtered_servers = [s for s in servers if s.is_sync_enabled]
        return filtered_servers


def start_dbt_multi_server_monitor(monitor_config: DbtMonitorConfig):
    runner = RUNNER_FACTORY[monitor_config.runner_type]
    DbtMultiServerMonitor(
        runner=runner,
        monitor_component_manager=DbtMonitorComponentManager,
        servers_configuration_service=get_servers_configuration_service(),
        monitor_config=monitor_config,
    ).run()
