import logging

from typing import Callable, Dict, Type

from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from airflow_monitor.shared.error_handler import capture_monitor_exception
from airflow_monitor.shared.runners import BaseRunner


logger = logging.getLogger(__name__)


class BaseMonitorComponentManager(object):
    def __init__(
        self,
        runner: Type[BaseRunner],
        server_config: BaseServerConfig,
        tracking_service: BaseDbndTrackingService,
        services_components: Dict[str, Callable] = {},
    ):
        self.runner = runner
        self.server_config = server_config
        self.tracking_service = tracking_service

        # Services to run
        self.services_components = services_components

        self.active_components: Dict[str, BaseRunner] = {}
        self._is_stopping: bool = False

    def _clean_dead_components(self):
        for name, component in list(self.active_components.items()):
            if not component.is_alive():
                component.stop()
                self.active_components.pop(name)

    def _is_component_enabled(self, component_name: str) -> bool:
        return getattr(self.server_config, f"{component_name}_enabled", False)

    def _should_stop_component(self, component_name: str) -> bool:
        if component_name not in self.active_components:
            return False
        if self._is_stopping:
            return True
        return not self._is_component_enabled(component_name)

    def _should_start_component(self, component_name: str) -> bool:
        if (
            self._is_component_enabled(component_name)
            and component_name not in self.active_components
            and not self._is_stopping
        ):
            return True

        return False

    def _update_component_state(self):
        self._clean_dead_components()

        is_monitored_server_alive = None
        for component_name, runnable in self.services_components.items():
            if self._should_start_component(component_name):
                if is_monitored_server_alive is None:
                    # check it only once per iteration and only if need to start anything
                    is_monitored_server_alive = self.is_monitored_server_alive()
                    if not is_monitored_server_alive:
                        logger.warning(
                            "Monitored Server is not responsive, will skip starting new syncers"
                        )
                if is_monitored_server_alive:
                    self.active_components[component_name] = self.runner(
                        target=runnable,
                        tracking_service=self.tracking_service,
                        tracking_source_uid=self.server_config.tracking_source_uid,
                    )
                    self.active_components[component_name].start()
            elif self._should_stop_component(component_name):
                component = self.active_components.pop(component_name)
                component.stop()

        self._set_running_monitor_state(is_monitored_server_alive)

    @capture_monitor_exception("stopping monitor")
    def stop(self):
        self._is_stopping = True
        self._update_component_state()

    @capture_monitor_exception("starting monitor")
    def start(self):
        self._set_starting_monitor_state()
        self._update_component_state()

    @capture_monitor_exception("updating monitor config")
    def update_config(self, server_config: BaseServerConfig):
        self.server_config = server_config
        self._update_component_state()

    @capture_monitor_exception("heartbeat monitor")
    def heartbeat(self):
        for component in self.active_components.values():
            component.heartbeat()

        self._clean_dead_components()

    @capture_monitor_exception("checking monitor alive")
    def is_monitored_server_alive(self):
        raise NotImplementedError()

    def _set_running_monitor_state(self, is_monitored_server_alive: bool):
        raise NotImplementedError()

    def _set_starting_monitor_state(self):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.__class__.__name__}({self.server_config.source_name}|{self.server_config.tracking_source_uid})"
