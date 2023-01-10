# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.fixer.runtime_fixer import AirflowRuntimeFixer
from airflow_monitor.multiserver.airflow_services_factory import (
    get_airflow_monitor_services_factory,
)
from airflow_monitor.shared.base_multiserver import MultiServerMonitor
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from dbnd._core.errors.base import DatabandConfigError


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

    services_components = {
        "state_sync": AirflowRuntimeSyncer,
        "fixer": AirflowRuntimeFixer,
        "config_updater": AirflowRuntimeConfigUpdater,
    }

    MultiServerMonitor(
        monitor_config=monitor_config,
        components_dict=services_components,
        monitor_services_factory=get_airflow_monitor_services_factory(),
    ).run()
