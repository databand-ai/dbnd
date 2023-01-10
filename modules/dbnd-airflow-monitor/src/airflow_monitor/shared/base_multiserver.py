# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from time import sleep
from typing import Dict, List, Type
from uuid import UUID

from urllib3.exceptions import HTTPError

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.shared.base_tracking_service import (
    BaseDbndTrackingService,
    WebServersConfigurationService,
)
from airflow_monitor.shared.error_handler import capture_monitor_exception
from airflow_monitor.shared.liveness_probe import create_liveness_file
from airflow_monitor.shared.logger_config import configure_logging
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from airflow_monitor.shared.sequential_runner import SequentialRunner
from dbnd._core.errors.base import DatabandConnectionException
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)


logger = logging.getLogger(__name__)


class MultiServerMonitor:
    servers_configuration_service: WebServersConfigurationService
    monitor_config: BaseMonitorConfig
    components_dict: Dict[str, Type[BaseMonitorSyncer]]
    monitor_services_factory: MonitorServicesFactory

    def __init__(
        self,
        monitor_config: BaseMonitorConfig,
        components_dict: Dict[str, Type[BaseMonitorSyncer]],
        monitor_services_factory: MonitorServicesFactory,
    ):
        self.monitor_config = monitor_config
        self.components_dict = components_dict
        self.monitor_services_factory = monitor_services_factory
        self.active_instances: Dict[UUID, Dict[str, SequentialRunner]] = {}

        self.iteration = 0
        self.stop_at = (
            utcnow() + timedelta(seconds=self.monitor_config.stop_after)
            if self.monitor_config.stop_after
            else None
        )

        self.monitor_configuration_service = (
            self.monitor_services_factory.get_servers_configuration_service()
        )

        self.tracking_services: Dict[UUID, BaseDbndTrackingService] = {}

    def _should_stop(self):
        if (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        ):
            return True

        if self.stop_at and utcnow() >= self.stop_at:
            return True

    def _stop_disabled_servers(self, servers_configs: List[BaseServerConfig]):
        server_ids = {s.tracking_source_uid for s in servers_configs}
        for server_id in list(self.active_instances.keys()):
            if server_id not in server_ids:
                self._stop(server_id)
                self.active_instances.pop(server_id)

    @retry(
        stop=stop_after_delay(30),
        retry=(
            retry_if_exception_type(HTTPError)
            | retry_if_exception_type(DatabandConnectionException)
        ),
        wait=wait_exponential(multiplier=1, max=10),
        before_sleep=before_sleep_log(logger, logging.DEBUG),
        reraise=True,
    )
    def _get_servers_configs_safe(self):
        return self.monitor_services_factory.get_servers_configuration_service().get_all_servers_configuration(
            self.monitor_config
        )

    def _ensure_monitored_servers(self, servers_configs: List[BaseServerConfig]):
        self._stop_disabled_servers(servers_configs)

        for server_config in servers_configs:
            server_id = server_config.tracking_source_uid
            instance = self.active_instances.get(server_id)
            if not instance:
                self.tracking_services[
                    server_id
                ] = self.monitor_services_factory.get_tracking_service(server_config)
                self._start(server_id)

    def _heartbeat(self):
        for server_id in self.active_instances.keys():
            logger.debug(
                "Starting new sync iteration for tracking_source_uid=%s, iteration %d",
                server_id,
                self.iteration,
            )
            for component in self.active_instances[server_id].values():
                component.heartbeat(is_last=False)

    def run(self):
        configure_logging(use_json=self.monitor_config.use_json_logging)

        while True:
            self.iteration += 1
            try:
                logger.debug("Starting %s iteration", self.iteration)
                self.run_once()
                self.monitor_configuration_service.send_metrics(self.monitor_config)
                logger.debug("Iteration %s done", self.iteration)
            except Exception:
                logger.exception("Unknown exception during iteration", exc_info=True)

            if self._should_stop():
                self._stop_disabled_servers([])
                break

            sleep(self.monitor_config.interval)

    def run_once(self):
        servers: List[BaseServerConfig] = self._get_servers_configs_safe()
        if not servers:
            logger.warning("No servers found")

        active_servers = [s for s in servers if s.is_sync_enabled]

        if not active_servers:
            logger.warning("No enabled servers found")

        self._ensure_monitored_servers(active_servers)
        self._heartbeat()
        create_liveness_file(self.monitor_configuration_service.monitor_type)

    @capture_monitor_exception("starting monitor")
    def _start(self, server_id: UUID):
        logger.info("Starting components for %s", server_id)
        self.tracking_services[server_id].set_starting_monitor_state()
        self._start_components(server_id)
        self.tracking_services[server_id].set_running_monitor_state()

    @capture_monitor_exception("stopping monitor")
    def _stop(self, server_id):
        logger.info("Stopping components for %s which is no longer enabled", server_id)
        self._stop_components(server_id)
        self.tracking_services.pop(server_id)

    def _start_components(self, server_id):
        self.active_instances[server_id] = {}
        tracking_service = self.tracking_services[server_id]

        for component_name, syncer_class in self.components_dict.items():
            monitor_config = self.tracking_services[
                server_id
            ].get_monitor_configuration()
            data_fetcher = self.monitor_services_factory.get_data_fetcher(
                monitor_config
            )
            syncer_instance: BaseMonitorSyncer = syncer_class(
                config=monitor_config,
                tracking_service=tracking_service,
                data_fetcher=data_fetcher,
            )
            component_with_runner = SequentialRunner(
                target=syncer_instance,
                tracking_service=tracking_service,
                tracking_source_uid=server_id,
            )
            self.active_instances[server_id][component_name] = component_with_runner

    def _stop_components(self, server_id):
        components_to_remove = []
        for name, component in self.active_instances[server_id].items():
            logger.warning("Running last heartbeat before stopping for %s", name)
            components_to_remove.append(name)
            component.heartbeat(is_last=True)

        for component_name in components_to_remove:
            self.active_instances[server_id].pop(component_name)
