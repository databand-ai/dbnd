import logging

from datetime import timedelta
from time import sleep
from typing import Dict, List, Type, Union
from uuid import UUID

from airflow_monitor.common import capture_monitor_exception
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.metric_reporter import generate_latest_metrics
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.liveness_probe import create_liveness_file
from airflow_monitor.multiserver.monitor_component_manager import (
    MonitorComponentManager,
)
from airflow_monitor.multiserver.runners import (
    RUNNER_FACTORY,
    MultiProcessRunner,
    SequentialRunner,
)
from airflow_monitor.tracking_service import (
    get_servers_configuration_service,
    get_tracking_service,
)
from airflow_monitor.tracking_service.base_tracking_service import (
    DbndAirflowTrackingService,
    ServersConfigurationService,
)
from dbnd._core.errors.base import DatabandConfigError, DatabandConnectionException
from dbnd._core.utils.timezone import utcnow
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)
from urllib3.exceptions import HTTPError


logger = logging.getLogger(__name__)


class MultiServerMonitor(object):
    runner_factory: Union[Type[SequentialRunner], Type[MultiProcessRunner], None]
    monitor_config: AirflowMonitorConfig
    active_monitors: Dict[UUID, MonitorComponentManager]
    tracking_service: DbndAirflowTrackingService

    def __init__(
        self,
        servers_configuration_service: ServersConfigurationService,
        monitor_config: AirflowMonitorConfig,
    ):
        self.servers_configuration_service = servers_configuration_service
        self.monitor_config = monitor_config
        self.runner_factory = RUNNER_FACTORY[self.monitor_config.runner_type]
        self.active_monitors = {}

        self.iteration = 0
        self.stop_at = (
            utcnow() + timedelta(seconds=self.monitor_config.stop_after)
            if self.monitor_config.stop_after
            else None
        )

    def run(self):
        if self.monitor_config.sql_alchemy_conn and not self.monitor_config.syncer_name:
            raise DatabandConfigError(
                "Syncer name should be specified when using direct sql connection",
                help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
            )

        while True:
            self.iteration += 1
            try:
                logger.info(f"Starting {self.iteration} iteration")
                self.run_once()
                self._send_metrics()
                logger.info(f"Iteration {self.iteration} done")
            except Exception:
                logger.exception("Unknown exception during iteration", exc_info=True)

            if self._should_stop():
                self.stop_disabled_servers([])
                break

            sleep(self.monitor_config.interval)

    @capture_monitor_exception
    def _send_metrics(self):
        metrics = generate_latest_metrics().decode("utf-8")
        self.servers_configuration_service.send_prometheus_metrics(
            self.monitor_config.syncer_name or "airflow-monitor", metrics
        )

    def _should_stop(self):
        if (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        ):
            return True

        if self.stop_at and utcnow() >= self.stop_at:
            return True

    def run_once(self):
        servers = self._get_servers_configs_safe()
        if not servers:
            logger.warning("No servers found")

        servers = self.filter_servers(servers)
        servers = [s for s in servers if s.is_sync_enabled and s.is_sync_enabled_v2]

        if not servers:
            logger.warning("No enabled servers found")

        self.ensure_monitored_servers(servers)
        self.heartbeat()
        create_liveness_file()

    def filter_servers(self, servers):
        if not self.monitor_config.syncer_name:
            return [s for s in servers if s.fetcher != "db"]

        servers_filtered = [
            s for s in servers if s.name == self.monitor_config.syncer_name
        ]
        if not servers_filtered:
            raise DatabandConfigError(
                "No syncer configuration found matching name '%s'. Available syncers: %s"
                % (
                    self.monitor_config.syncer_name,
                    ",".join([s.name for s in servers if s.name]),
                ),
                help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
            )
        return servers_filtered

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

    def ensure_monitored_servers(self, servers_configs: List[AirflowServerConfig]):
        self.stop_disabled_servers(servers_configs)

        for server_config in servers_configs:
            server_id = server_config.tracking_source_uid
            monitor = self.active_monitors.get(server_id)
            if monitor:
                monitor.update_config(server_config)
            else:
                logger.info(f"Starting new monitor for {server_id}")
                monitor = MonitorComponentManager(
                    server_config,
                    self.runner_factory,
                    tracking_service=get_tracking_service(server_id),
                )
                monitor.start()
                self.active_monitors[server_id] = monitor

    def stop_disabled_servers(self, servers_configs):
        servers_ids = {s.tracking_source_uid for s in servers_configs}
        for server_id, monitor in list(
            self.active_monitors.items()
        ):  # type: (UUID, MonitorComponentManager)
            if server_id not in servers_ids:
                logger.info(f"Server {server_id} not longer enabled, going to stop it")
                monitor.stop()
                self.active_monitors.pop(server_id)

    def heartbeat(self):
        for monitor in self.active_monitors.values():
            monitor.heartbeat()


def start_multi_server_monitor(monitor_config: AirflowMonitorConfig):
    MultiServerMonitor(get_servers_configuration_service(), monitor_config).run()
