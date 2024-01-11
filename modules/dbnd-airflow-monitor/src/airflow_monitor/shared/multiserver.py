# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from time import sleep
from typing import Callable, Iterable, List, Tuple, Type, TypeVar
from uuid import UUID

from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_integration import BaseIntegration
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from airflow_monitor.shared.liveness_probe import create_liveness_file
from airflow_monitor.shared.logger_config import configure_logging
from airflow_monitor.shared.monitoring.newrelic import transaction_scope
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


class MultiServerMonitor:
    """
    // Generated by WCA for GP
    MultiServerMonitor is a class that runs a periodic loop to sync data for defined
    integrations.  It is responsible for starting and stopping integrations, syncing
    data, and sending metrics to the backend server.

    When the monitor starts, it gets a list of all the integrations of the specified
    types from the integration management service. It stops any integrations that are no
    longer enabled in the configuration, starts any new integrations that are enabled,
    runs a sync iteration for each integration, and sends metrics to the backend server.

    The monitor also checks the intervals of each component and ensures that they are
    running at the correct interval.
    """

    def __init__(
        self,
        monitor_config: BaseMonitorConfig,
        integration_management_service: IntegrationManagementService,
        integration_types: List[Type[BaseIntegration]],
    ) -> None:
        self.monitor_config = monitor_config
        self.active_integrations = {}
        self.current_integrations = []
        self.integration_types = integration_types

        self.iteration = 0
        self.stop_at = (
            utcnow() + timedelta(seconds=self.monitor_config.stop_after)
            if self.monitor_config.stop_after
            else None
        )

        self.integration_management_service = integration_management_service

    def _should_stop(self):
        if (
            self.monitor_config.number_of_iterations
            and self.iteration >= self.monitor_config.number_of_iterations
        ):
            return True

        if self.stop_at and utcnow() >= self.stop_at:
            return True

        return False

    def _stop_disabled_integrations(self, integrations: List[BaseIntegration]):
        for integration in integrations:
            logger.info("Stopping disabled integration %s", integration.config.uid)
            integration.on_integration_disabled()
            self.active_integrations.pop(integration.config.uid)

    def _start_new_enabled_integrations(self, integrations: List[BaseIntegration]):
        for integration in integrations:
            integration_uid = integration.config.uid
            if integration_uid not in self.active_integrations:
                logger.info("Started syncing new integration %s", integration_uid)
                self.active_integrations[integration_uid] = {}
                integration.on_integration_enabled()

    def _component_interval_is_met(
        self, integration_uid: UUID, component: BaseComponent
    ) -> bool:
        """
        Every component has an interval, make sure it doesn't run more often than the interval
        """
        last_heartbeat = self.active_integrations[integration_uid].get(
            component.identifier
        )
        if last_heartbeat is None:
            return True

        time_from_last_heartbeat = (utcnow() - last_heartbeat).total_seconds()
        return time_from_last_heartbeat >= component.sleep_interval

    def _heartbeat(self, integrations: List[BaseIntegration]):
        for integration in integrations:
            integration_uid = integration.config.uid
            logger.debug(
                "Starting new sync iteration for integration_uid=%s, iteration %d",
                integration_uid,
                self.iteration,
            )
            # create new syncers with new config every heartbeat
            components_list = integration.get_components()
            for component in components_list:
                if self._component_interval_is_met(integration_uid, component):
                    component.refresh_config(integration.config)
                    component.sync_once()
                    self.active_integrations[integration_uid][
                        component.identifier
                    ] = utcnow()

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
                self._stop_disabled_integrations(self.current_integrations)
                break

            sleep(self.monitor_config.interval)

    def partition_integrations(
        self, new_integrations: List[BaseIntegration]
    ) -> Tuple[List[BaseIntegration], List[BaseIntegration]]:
        to_add = exclude_by_key(
            new_integrations, self.current_integrations, lambda i: i.config.uid
        )

        to_remove = exclude_by_key(
            self.current_integrations, new_integrations, lambda i: i.config.uid
        )

        return to_add, to_remove

    def run_once(self):
        with transaction_scope("refresh_integrations"):
            integrations = self.get_integrations()
            to_add, to_remove = self.partition_integrations(integrations)
            self.current_integrations = integrations
            self._stop_disabled_integrations(to_remove)
            self._start_new_enabled_integrations(to_add)
        self._heartbeat(integrations)
        create_liveness_file()

    def get_integrations(self) -> List[BaseIntegration]:
        integrations = []
        for integration_type in self.integration_types:
            configs = self.integration_management_service.get_all_integration_configs(
                integration_type.MONITOR_TYPE, self.monitor_config.syncer_name
            )
            integrations.extend(
                [
                    integration_type.build_integration(config, self.monitor_config)
                    for config in configs
                ]
            )
        return integrations


T = TypeVar("T")


# Generated by WCA for GP
def exclude_by_key(
    list_a: Iterable[T], list_b: Iterable[T], key: Callable[[T], bool]
) -> List[T]:
    """
    Returns a list of elements in list_a that are not present in list_b,
    where the comparison is made using the given key function.
    """
    b_keys = {key(item) for item in list_b}
    return [item for item in list_a if key(item) not in b_keys]
