import logging

from typing import Any, Callable, Dict

import airflow_monitor

from airflow_monitor.common import MonitorState, capture_monitor_exception
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher import get_data_fetcher
from airflow_monitor.fixer.runtime_fixer import start_runtime_fixer
from airflow_monitor.multiserver.runners.base_runner import BaseRunner
from airflow_monitor.syncer.runtime_syncer import start_runtime_syncer
from airflow_monitor.tracking_service import DbndAirflowTrackingService
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)

KNOWN_COMPONENTS = {
    "state_sync": start_runtime_syncer,
    # "xcom_sync": Component,
    # "dag_sync": Component,
    "fixer": start_runtime_fixer,
}


class MonitorComponentManager(object):
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
        self.plugin_metadata = None

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

        if airflow_alive and self.plugin_metadata is None:
            self._set_monitor_state()

    def _clean_dead_components(self):
        for name, component in list(self.active_components.items()):
            if not component.is_alive():
                component.stop()
                self.active_components.pop(name)

    @capture_monitor_exception("stopping monitor")
    def stop(self):
        self._is_stopping = True
        self._update_component_state()

    def _set_monitor_state(self):
        plugin_metadata = get_data_fetcher(self.server_config).get_plugin_metadata()

        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_start_time=utcnow(),
                monitor_status="Running",
                airflow_monitor_version=airflow_monitor.__version__ + " v2",
                airflow_version=plugin_metadata.airflow_version,
                airflow_export_version=plugin_metadata.plugin_version,
                airflow_instance_uid=plugin_metadata.airflow_instance_uid,
            ),
        )

        self.plugin_metadata = plugin_metadata

    @capture_monitor_exception("starting monitor")
    def start(self):
        self.tracking_service.update_monitor_state(
            MonitorState(
                monitor_start_time=utcnow(),
                monitor_status="Scheduled",
                airflow_monitor_version=airflow_monitor.__version__ + " v2",
                monitor_error_message=None,
            ),
        )
        self._update_component_state()

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
        return f"{self.__class__.__name__}({self.server_config.name}|{self.server_config.tracking_source_uid})"
