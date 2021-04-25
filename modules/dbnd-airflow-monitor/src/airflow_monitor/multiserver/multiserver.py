import logging

from datetime import timedelta
from time import sleep
from typing import Any, Callable, Dict, List, Type, Union
from uuid import UUID

import airflow_monitor

from airflow_monitor.common import MonitorState, capture_monitor_exception
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.data_fetcher import get_data_fetcher
from airflow_monitor.fixer.runtime_fixer import start_runtime_fixer
from airflow_monitor.multiserver.runners import (
    RUNNER_FACTORY,
    MultiProcessRunner,
    SequentialRunner,
)
from airflow_monitor.multiserver.runners.base_runner import BaseRunner
from airflow_monitor.syncer.runtime_syncer import start_runtime_syncer
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

KNOWN_COMPONENTS = {
    "state_sync": start_runtime_syncer,
    # "xcom_sync": Component,
    # "dag_sync": Component,
    "fixer": start_runtime_fixer,
}


class AirflowMonitor(object):
    runner_factory: Callable[[Any], BaseRunner]

    def __init__(
        self,
        server_config: AirflowServerConfig,
        runner_factory: Callable[..., BaseRunner],
        tracking_service: DbndAirflowTrackingService,
    ):
        self.server_config = server_config
        self.active_components = {}  # type: Dict[str, BaseRunner]
        self.tracking_service = tracking_service

        self.runner_factory = runner_factory
        self._is_stopping = False

    def is_enabled(self, component_name):
        if self._is_stopping:
            return False
        return getattr(self.server_config, f"{component_name}_enabled", False)

    def is_active(self, component_name):
        return component_name in self.active_components

    def _update_component_state(self):
        self._clean_dead_components()

        airflow_alive = None
        for name, runnable in KNOWN_COMPONENTS.items():
            if self.is_enabled(name) and not self.is_active(name):
                if airflow_alive is None:
                    # check it only once per iteration and only if need to start anything
                    airflow_alive = self.is_airflow_server_alive()
                    if not airflow_alive:
                        logger.warning(
                            "Airflow Server is not responsive, will skip starting new syncers"
                        )
                if airflow_alive:
                    self.active_components[name] = self.runner_factory(
                        target=runnable,
                        tracking_service=self.tracking_service,
                        tracking_source_uid=self.server_config.tracking_source_uid,
                    )
                    self.active_components[name].start()
            elif not self.is_enabled(name) and self.is_active(name):
                component = self.active_components.pop(name)
                component.stop()

        return airflow_alive

    def _clean_dead_components(self):
        for name, component in list(self.active_components.items()):
            if not component.is_alive():
                component.stop()
                self.active_components.pop(name)

    @capture_monitor_exception("stopping monitor")
    def stop(self):
        self._is_stopping = True
        self._update_component_state()

    @capture_monitor_exception("starting monitor")
    def start(self):
        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_start_time=utcnow(),
                monitor_status="Scheduled",
                airflow_monitor_version=airflow_monitor.__version__,
                monitor_error_message=None,
            ),
        )
        alive = self._update_component_state()

        if alive:
            plugin_metadata = get_data_fetcher(self.server_config).get_plugin_metadata()
        else:
            plugin_metadata = None

        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_start_time=utcnow(),
                monitor_status="Running",
                airflow_monitor_version=airflow_monitor.__version__,
                airflow_version=plugin_metadata.airflow_version
                if plugin_metadata
                else None,
                airflow_export_version=plugin_metadata.plugin_version
                if plugin_metadata
                else None,
            ),
        )

    @capture_monitor_exception("updating monitor config")
    def update_config(self, server_config: AirflowServerConfig):
        self.server_config = server_config
        self._update_component_state()

    @capture_monitor_exception("checking monitor alive")
    def is_airflow_server_alive(self):
        return get_data_fetcher(self.server_config).is_alive()

    @capture_monitor_exception("heartbeat monitor")
    def heartbeat(self):
        for component in self.active_components.values():
            component.heartbeat()

        self._clean_dead_components()

    def __str__(self):
        return f"AirflowMonitor({self.server_config.name}|{self.server_config.tracking_source_uid})"


class MultiServerMonitor(object):
    runner_factory: Union[Type[SequentialRunner], Type[MultiProcessRunner], None]
    monitor_config: AirflowMonitorConfig
    active_monitors: Dict[UUID, AirflowMonitor]
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
                logger.info(f"Iteration {self.iteration} done")
            except Exception:
                logger.exception("Unknown exception during iteration", exc_info=True)

            if self._should_stop():
                self.stop_disabled_servers([])
                break

            sleep(self.monitor_config.interval)

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

    def filter_servers(self, servers):
        if not self.monitor_config.syncer_name:
            return servers

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
                monitor = AirflowMonitor(
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
        ):  # type: (UUID, AirflowMonitor)
            if server_id not in servers_ids:
                logger.info(f"Server {server_id} not longer enabled, going to stop it")
                monitor.stop()
                self.active_monitors.pop(server_id)

    def heartbeat(self):
        for monitor in self.active_monitors.values():
            monitor.heartbeat()


def start_multi_server_monitor(monitor_config: AirflowMonitorConfig):
    MultiServerMonitor(get_servers_configuration_service(), monitor_config).run()
