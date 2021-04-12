import logging

from time import sleep
from typing import Any, Callable, Dict, List, Type, Union
from uuid import UUID

from airflow_monitor.common import MonitorState, capture_monitor_exception
from airflow_monitor.common.config_data import (
    AirflowServerConfig,
    MultiServerMonitorConfig,
)
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
from airflow_monitor.tracking_service.af_tracking_service import (
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

        airflow_alive = self.is_airflow_server_alive()
        if not airflow_alive:
            logger.warning(
                "Airflow Server is not responsive, will skip starting new syncers"
            )

        for name, runnable in KNOWN_COMPONENTS.items():
            if airflow_alive and self.is_enabled(name) and not self.is_active(name):
                self.active_components[name] = self.runner_factory(
                    target=runnable,
                    tracking_service=self.tracking_service,
                    tracking_source_uid=self.server_config.tracking_source_uid,
                )
                self.active_components[name].start()
            elif not self.is_enabled(name) and self.is_active(name):
                component = self.active_components.pop(name)
                component.stop()

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
        self._update_component_state()

        self.tracking_service.update_monitor_state(
            MonitorState(monitor_start_time=utcnow(), monitor_status="Running"),
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
    monitor_config: MultiServerMonitorConfig
    active_monitors: Dict[UUID, AirflowMonitor]
    tracking_service: DbndAirflowTrackingService

    def __init__(
        self,
        servers_configuration_service: ServersConfigurationService,
        monitor_config: MultiServerMonitorConfig,
    ):
        self.servers_configuration_service = servers_configuration_service
        self.monitor_config = monitor_config
        self.runner_factory = RUNNER_FACTORY[self.monitor_config.runner_type]
        self.active_monitors = {}
        self.iteration = 0

    def run(self):
        while not self._should_stop():
            self.iteration += 1
            try:
                logger.info(f"Starting {self.iteration} iteration")
                self.run_once()
                logger.info(f"Iteration {self.iteration} done")
            except Exception:
                logger.exception("Unknown exception during iteration")
                if self.iteration == 1:
                    raise
            if not self._should_stop():
                sleep(self.monitor_config.interval)

    def _should_stop(self):
        return (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        )

    def run_once(self):
        servers = self._get_servers_configs_safe()
        servers = self.filter_servers(servers)
        servers = [s for s in servers if s.is_sync_enabled and s.is_sync_enabled_v2]

        if not servers:
            logger.warning("No enabled servers found")

        self.ensure_monitored_servers(servers)
        self.heartbeat()

    def filter_servers(self, servers):
        if not self.monitor_config.syncer_names:
            return servers

        servers_filtered = [
            s for s in servers if s.name in self.monitor_config.syncer_names
        ]
        if len(servers_filtered) != len(self.monitor_config.syncer_names):
            filtered_names = {s.name for s in servers_filtered}
            missing_names = ",".join(
                [
                    name
                    for name in self.monitor_config.syncer_names
                    if name not in filtered_names
                ]
            )
            msg = f"No configuration found for monitored servers: {missing_names}"
            if self.iteration == 1:
                raise DatabandConfigError(
                    msg,
                    help_msg="Please make sure you've properly setup configuration"
                    " the specified airflow servers in Databand UI",
                )
            else:
                logger.warning(msg)
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
        return self.servers_configuration_service.get_all_servers_configuration()

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


def start_multi_server_monitor(
    interval=10, runner_type="seq", syncer_name=None, number_of_iterations=None,
):
    MultiServerMonitor(
        get_servers_configuration_service(),
        MultiServerMonitorConfig(
            interval=interval,
            runner_type=runner_type,
            number_of_iterations=number_of_iterations,
            syncer_names=syncer_name,
        ),
    ).run()
