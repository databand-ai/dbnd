import logging

from typing import Type
from uuid import UUID

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.metric_reporter import generate_latest_metrics
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.monitor_component_manager import (
    AirflowMonitorComponentManager,
)
from airflow_monitor.shared.base_multiserver import BaseMultiServerMonitor
from airflow_monitor.shared.base_tracking_service import WebServersConfigurationService
from airflow_monitor.shared.runners import RUNNER_FACTORY, BaseRunner
from airflow_monitor.tracking_service import (
    get_servers_configuration_service,
    get_tracking_service,
)
from airflow_monitor.tracking_service.web_tracking_service import (
    AirflowDbndTrackingService,
)
from dbnd._core.errors.base import DatabandConfigError


logger = logging.getLogger(__name__)


class AirflowMultiServerMonitor(BaseMultiServerMonitor):
    runner: Type[BaseRunner]
    monitor_component_manager: Type[AirflowMonitorComponentManager]
    servers_configuration_service: WebServersConfigurationService
    monitor_config: AirflowMonitorConfig

    def __init__(
        self,
        runner: Type[BaseRunner],
        monitor_component_manager: Type[AirflowMonitorComponentManager],
        servers_configuration_service: WebServersConfigurationService,
        monitor_config: AirflowMonitorConfig,
    ):
        super(AirflowMultiServerMonitor, self).__init__(
            runner,
            monitor_component_manager,
            servers_configuration_service,
            monitor_config,
        )

    @capture_monitor_exception
    def _send_metrics(self):
        metrics = generate_latest_metrics().decode("utf-8")
        self.servers_configuration_service.send_prometheus_metrics(
            metrics, self.monitor_config.syncer_name
        )

    def _assert_valid_config(self):
        if self.monitor_config.sql_alchemy_conn and not self.monitor_config.syncer_name:
            raise DatabandConfigError(
                "Syncer name should be specified when using direct sql connection",
                help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
            )

    def _get_tracking_service(
        self, tracking_source_uid: UUID
    ) -> AirflowDbndTrackingService:
        return get_tracking_service(tracking_source_uid)

    def _filter_servers(self, servers):
        if not self.monitor_config.syncer_name:
            filtered_servers = [s for s in servers if s.fetcher_type != "db"]
        else:
            filtered_servers = [
                s for s in servers if s.source_name == self.monitor_config.syncer_name
            ]
            if not filtered_servers:
                raise DatabandConfigError(
                    "No syncer configuration found matching name '%s'. Available syncers: %s"
                    % (
                        self.monitor_config.syncer_name,
                        ",".join([s.source_name for s in servers if s.source_name]),
                    ),
                    help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                    " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
                )
        return [s for s in filtered_servers if s.is_sync_enabled]


def start_multi_server_monitor(monitor_config: AirflowMonitorConfig):
    runner = RUNNER_FACTORY[monitor_config.runner_type]
    AirflowMultiServerMonitor(
        runner=runner,
        monitor_component_manager=AirflowMonitorComponentManager,
        servers_configuration_service=get_servers_configuration_service(),
        monitor_config=monitor_config,
    ).run()
