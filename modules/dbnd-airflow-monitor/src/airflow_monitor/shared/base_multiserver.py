import logging

from datetime import timedelta
from time import sleep
from typing import Dict, List, Type
from uuid import UUID

from urllib3.exceptions import HTTPError

from airflow_monitor.shared.base_monitor_component_manager import (
    BaseMonitorComponentManager,
)
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import (
    BaseDbndTrackingService,
    WebServersConfigurationService,
)
from airflow_monitor.shared.liveness_probe import create_liveness_file
from airflow_monitor.shared.runners import BaseRunner
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


class BaseMultiServerMonitor(object):
    runner: Type[BaseRunner]
    monitor_component_manager: Type[BaseMonitorComponentManager]
    servers_configuration_service: WebServersConfigurationService
    monitor_config: BaseMonitorConfig

    def __init__(
        self,
        runner: Type[BaseRunner],
        monitor_component_manager: Type[BaseMonitorComponentManager],
        servers_configuration_service: WebServersConfigurationService,
        monitor_config: BaseMonitorConfig,
    ):
        self.runner = runner
        self.monitor_component_manager = monitor_component_manager
        self.servers_configuration_service = servers_configuration_service
        self.monitor_config = monitor_config
        self.active_monitors: Dict[UUID, BaseMonitorComponentManager] = {}

        self.iteration = 0
        self.stop_at = (
            utcnow() + timedelta(seconds=self.monitor_config.stop_after)
            if self.monitor_config.stop_after
            else None
        )

    def _should_stop(self):
        if (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        ):
            return True

        if self.stop_at and utcnow() >= self.stop_at:
            return True

    def _stop_disabled_servers(self, servers_configs: List[BaseServerConfig]):
        servers_ids = {s.tracking_source_uid for s in servers_configs}
        for server_id, monitor in list(
            self.active_monitors.items()
        ):  # type: (UUID, BaseMonitorComponentManager)
            if server_id not in servers_ids:
                logger.info(
                    f"Server: {monitor.server_config.source_name} ({server_id}) not longer enabled.\nStoping..."
                )
                monitor.stop()
                self.active_monitors.pop(server_id)

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
        return self.servers_configuration_service.get_all_servers_configuration(
            self.monitor_config
        )

    def _ensure_monitored_servers(self, servers_configs: List[BaseServerConfig]):
        self._stop_disabled_servers(servers_configs)

        for server_config in servers_configs:
            server_id = server_config.tracking_source_uid
            monitor = self.active_monitors.get(server_id)
            if monitor:
                monitor.update_config(server_config)
            else:
                logger.info("Starting new monitor for %s", server_id)
                monitor = self.monitor_component_manager(
                    self.runner,
                    server_config,
                    tracking_service=self._get_tracking_service(server_id),
                )
                monitor.start()
                self.active_monitors[server_id] = monitor

    def _heartbeat(self):
        for monitor in self.active_monitors.values():
            logger.debug(
                "Starting new sync iteration for tracking_source_uid=%s, name=%s, iteration %d",
                monitor.server_config.tracking_source_uid,
                monitor.server_config.source_name,
                self.iteration,
            )
            monitor.heartbeat()

    def run(self):
        self._assert_valid_config()

        while True:
            self.iteration += 1
            try:
                logger.debug("Starting %s iteration", self.iteration)
                self.run_once()
                self._send_metrics()
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

        active_servers = self._filter_servers(servers)

        if not active_servers:
            logger.warning("No enabled servers found")

        self._ensure_monitored_servers(active_servers)
        self._heartbeat()
        create_liveness_file(self.servers_configuration_service.monitor_type)

    def _send_metrics(self):
        raise NotImplementedError()

    def _assert_valid_config(self):
        raise NotImplementedError()

    def _get_tracking_service(
        self, tracking_source_uid: UUID
    ) -> BaseDbndTrackingService:
        raise NotImplementedError()

    def _filter_servers(self, servers):
        raise NotImplementedError()
