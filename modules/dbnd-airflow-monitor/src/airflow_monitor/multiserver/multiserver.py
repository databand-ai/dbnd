# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.airflow_integration import AirflowIntegration
from dbnd._core.errors.base import DatabandConfigError
from dbnd_monitor.integration_management_service import IntegrationManagementService
from dbnd_monitor.multiserver import MultiServerMonitor


logger = logging.getLogger(__name__)


def assert_valid_config(monitor_config):
    if monitor_config.sql_alchemy_conn and not monitor_config.syncer_name:
        raise DatabandConfigError(
            "Syncer name should be specified when using direct sql connection",
            help_msg="Please provide correct syncer name (using --syncer-name parameter,"
            " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
        )


def start_multi_server_monitor(monitor_config: AirflowMonitorConfig):
    assert_valid_config(monitor_config)

    MultiServerMonitor(
        monitor_config=monitor_config,
        integration_management_service=IntegrationManagementService(),
        integration_types=[AirflowIntegration],
    ).run()
