# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, Type

from airflow_monitor.common.errors import capture_monitor_exception
from airflow_monitor.common.metric_reporter import generate_latest_metrics
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.fixer.runtime_fixer import AirflowRuntimeFixer
from airflow_monitor.multiserver.airflow_services_factory import (
    get_airflow_monitor_services_factory,
)
from airflow_monitor.multiserver.monitor_component_manager import (
    AirflowMonitorComponentManager,
)
from airflow_monitor.shared.base_multiserver import BaseMultiServerMonitor
from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.shared.base_tracking_service import WebServersConfigurationService
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from airflow_monitor.shared.runners import BaseRunner
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from dbnd._core.errors.base import DatabandConfigError


logger = logging.getLogger(__name__)


class AirflowMultiServerMonitor(BaseMultiServerMonitor):
    runner: Type[BaseRunner]
    monitor_component_manager: Type[AirflowMonitorComponentManager]
    servers_configuration_service: WebServersConfigurationService
    monitor_config: AirflowMonitorConfig
    components_dict: Dict[str, Type[BaseMonitorSyncer]]
    monitor_services_factory: MonitorServicesFactory

    def __init__(
        self,
        monitor_component_manager: Type[AirflowMonitorComponentManager],
        monitor_config: AirflowMonitorConfig,
        components_dict: Dict[str, Type[BaseMonitorSyncer]],
        monitor_services_factory: MonitorServicesFactory,
    ):
        super(AirflowMultiServerMonitor, self).__init__(
            monitor_component_manager,
            monitor_config,
            components_dict,
            monitor_services_factory,
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
    services_components = {
        "state_sync": AirflowRuntimeSyncer,
        "fixer": AirflowRuntimeFixer,
        "config_updater": AirflowRuntimeConfigUpdater,
    }

    AirflowMultiServerMonitor(
        monitor_component_manager=AirflowMonitorComponentManager,
        monitor_config=monitor_config,
        components_dict=services_components,
        monitor_services_factory=get_airflow_monitor_services_factory(),
    ).run()
