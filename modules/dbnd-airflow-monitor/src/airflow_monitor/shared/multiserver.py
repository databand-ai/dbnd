# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from time import sleep
from typing import Dict, List
from uuid import UUID

from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.liveness_probe import create_liveness_file
from airflow_monitor.shared.logger_config import configure_logging
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


class MultiServerMonitor:
    monitor_config: BaseMonitorConfig
    monitor_services_factory: MonitorServicesFactory

    def __init__(
        self,
        monitor_config: BaseMonitorConfig,
        monitor_services_factory: MonitorServicesFactory,
    ):
        self.monitor_config = monitor_config
        self.monitor_services_factory = monitor_services_factory
        self.active_instances: Dict[UUID, List[BaseComponent]] = {}

        self.iteration = 0
        self.stop_at = (
            utcnow() + timedelta(seconds=self.monitor_config.stop_after)
            if self.monitor_config.stop_after
            else None
        )

        self.syncer_management_service = (
            self.monitor_services_factory.get_syncer_management_service()
        )

    def _should_stop(self):
        if (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        ):
            return True

        if self.stop_at and utcnow() >= self.stop_at:
            return True

        return False

    def _stop_disabled_servers(self, servers_configs: List[BaseServerConfig]):
        server_ids = {s.identifier for s in servers_configs}
        for server_id in list(self.active_instances.keys()):
            if server_id not in server_ids:
                for component in self.active_instances[server_id]:
                    component.stop()
                self.active_instances.pop(server_id)

    def _start_new_enabled_servers(self, servers_configs: List[BaseServerConfig]):
        for server_config in servers_configs:
            server_id = server_config.identifier
            instance = self.active_instances.get(server_id)
            if not instance:
                server_id = server_config.identifier
                logger.info("Starting components for %s", server_id)
                self.syncer_management_service.set_starting_monitor_state(server_id)

                self.active_instances[server_id] = []
                components = self.monitor_services_factory.get_components(
                    server_config, self.syncer_management_service
                )
                self.active_instances[server_id] = components
                self.syncer_management_service.set_running_monitor_state(server_id)

    def _component_interval_is_met(self, component):
        """
        Every component has an interval, make sure it doesn't run more often than the interval
        """
        if component.last_heartbeat is None:
            return True

        time_from_last_heartbeat = (utcnow() - component.last_heartbeat).total_seconds()
        return time_from_last_heartbeat >= component.sleep_interval

    def _heartbeat(self, config_list):
        for config in config_list:
            server_id = config.identifier
            logger.debug(
                "Starting new sync iteration for server_id=%s, iteration %d",
                server_id,
                self.iteration,
            )
            for component in self.active_instances[server_id]:
                if self._component_interval_is_met(component):
                    component.refresh_config(config)
                    component.sync_once()
                    component.last_heartbeat = utcnow()

    def run(self):
        configure_logging(use_json=self.monitor_config.use_json_logging)

        while True:
            self.iteration += 1
            try:
                logger.debug("Starting %s iteration", self.iteration)
                self.run_once()
                self.syncer_management_service.send_metrics(self.monitor_config)
                logger.debug("Iteration %s done", self.iteration)
            except Exception:
                logger.exception("Unknown exception during iteration", exc_info=True)

            if self._should_stop():
                self._stop_disabled_servers([])
                break

            sleep(self.monitor_config.interval)

    def run_once(self):
        server_configs: List[
            BaseServerConfig
        ] = self.syncer_management_service.get_all_servers_configuration(
            self.monitor_config
        )
        self._stop_disabled_servers(server_configs)
        self._start_new_enabled_servers(server_configs)
        self._heartbeat(server_configs)
        create_liveness_file()
