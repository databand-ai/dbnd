# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from time import sleep
from typing import Dict, List
from uuid import UUID

import airflow_monitor

from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
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

        self.integration_management_service: IntegrationManagementService = (
            self.monitor_services_factory.get_integration_management_service()
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

    def _stop_disabled_servers(self, integration_configs: List[BaseServerConfig]):
        integration_uids = {s.uid for s in integration_configs}
        for integration_uid in list(self.active_instances.keys()):
            if integration_uid not in integration_uids:
                for component in self.active_instances[integration_uid]:
                    component.stop()
                self.active_instances.pop(integration_uid)

    def _start_new_enabled_servers(self, integration_configs: List[BaseServerConfig]):
        for integration_config in integration_configs:
            integration_uid = integration_config.uid
            instance = self.active_instances.get(integration_uid)
            if not instance:
                logger.info("Starting components for %s", integration_uid)

                components = self.monitor_services_factory.get_components(
                    integration_config, self.integration_management_service
                )
                self.active_instances[integration_uid] = components

                self._report_third_party_data(integration_config)

    def _report_third_party_data(self, integration_config):

        # This is the version of the monitor, since currently the shared logic exists
        # in airflow_monitor package we import this package and get the version
        metadata = {"monitor_version": airflow_monitor.__version__}

        adapter = self.monitor_services_factory.get_adapter(integration_config)
        if adapter:
            third_party_info = adapter.get_third_party_info()

            if third_party_info and third_party_info.error_list:
                formatted_error_list = ", ".join(third_party_info.error_list)
                self.integration_management_service.report_error(
                    integration_config.uid,
                    f"verify_environment_{integration_config.uid}",
                    formatted_error_list,
                )

            if third_party_info and third_party_info.metadata:
                metadata.update(third_party_info.metadata)

        self.integration_management_service.report_metadata(
            integration_config.uid, metadata
        )

    def _component_interval_is_met(self, component: BaseComponent) -> bool:
        """
        Every component has an interval, make sure it doesn't run more often than the interval
        """
        if component.last_heartbeat is None:
            return True

        time_from_last_heartbeat = (utcnow() - component.last_heartbeat).total_seconds()
        return time_from_last_heartbeat >= component.sleep_interval

    def _create_new_components(self, integration_config: BaseServerConfig):
        integration_uid = integration_config.uid
        existing_components = self.active_instances[integration_uid]
        new_components = self.monitor_services_factory.get_components(
            integration_config=integration_config,
            integration_management_service=self.integration_management_service,
        )
        existing_components_dict = {
            component.identifier: component for component in existing_components
        }
        new_components_dict = {
            component.identifier: component for component in new_components
        }

        # Keep heartbeats from existing components
        for component_id, component in new_components_dict.items():
            if component_id in existing_components_dict:
                component.last_heartbeat = existing_components_dict[
                    component_id
                ].last_heartbeat

        self.active_instances[integration_uid] = new_components

    def _heartbeat(self, integration_configs: list[BaseServerConfig]):
        for integration_config in integration_configs:
            integration_uid = integration_config.uid
            logger.debug(
                "Starting new sync iteration for integration_uid=%s, iteration %d",
                integration_uid,
                self.iteration,
            )
            # create new syncers with new config every heartbeat
            self._create_new_components(integration_config)
            for component in self.active_instances[integration_uid]:
                if self._component_interval_is_met(component):
                    component.refresh_config(integration_config)
                    component.sync_once()
                    component.last_heartbeat = utcnow()

    def run(self):
        configure_logging(use_json=self.monitor_config.use_json_logging)

        while True:
            self.iteration += 1
            try:
                logger.debug("Starting %s iteration", self.iteration)
                self.run_once()
                name = getattr(self.monitor_config, "syncer_name", None)
                self.integration_management_service.send_metrics(name)
                logger.debug("Iteration %s done", self.iteration)
            except Exception:
                logger.exception("Unknown exception during iteration", exc_info=True)

            if self._should_stop():
                self._stop_disabled_servers([])
                break

            sleep(self.monitor_config.interval)

    def run_once(self):
        integration_configs: List[
            BaseServerConfig
        ] = self.integration_management_service.get_all_servers_configuration(
            self.monitor_config
        )
        self._stop_disabled_servers(integration_configs)
        self._start_new_enabled_servers(integration_configs)
        self._heartbeat(integration_configs)
        create_liveness_file()
