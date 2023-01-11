# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.plugin_metadata import get_plugin_metadata
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher
from airflow_monitor.fixer.runtime_fixer import AirflowRuntimeFixer
from airflow_monitor.shared.decorators import (
    decorate_configuration_service,
    decorate_fetcher,
    decorate_tracking_service,
)
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from airflow_monitor.tracking_service.airflow_syncer_management_service import (
    AirflowSyncerManagementService,
)
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)
from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.basics.memoized import cached


FETCHERS = {"db": DbFetcher, "web": WebFetcher}


MONITOR_TYPE = "airflow"


class AirflowServicesFactory(MonitorServicesFactory):
    def get_components_dict(self):
        return {
            "state_sync": AirflowRuntimeSyncer,
            "fixer": AirflowRuntimeFixer,
            "config_updater": AirflowRuntimeConfigUpdater,
        }

    def get_data_fetcher(self, server_config):
        fetcher = FETCHERS.get(server_config.fetcher_type)
        if fetcher:
            return decorate_fetcher(fetcher(server_config), server_config.base_url)

        err = "Unsupported fetcher_type: {}, use one of the following: {}".format(
            server_config.fetcher_type, "/".join(FETCHERS.keys())
        )
        raise DatabandConfigError(err, help_msg="Please specify correct fetcher type")

    @cached()
    def get_syncer_management_service(self) -> AirflowSyncerManagementService:
        plugin_metadata = get_plugin_metadata()
        return decorate_configuration_service(
            AirflowSyncerManagementService(
                monitor_type=MONITOR_TYPE,
                server_monitor_config=AirflowServerConfig,
                plugin_metadata=plugin_metadata,
            )
        )

    def get_tracking_service(self, server_config) -> AirflowTrackingService:
        return decorate_tracking_service(
            AirflowTrackingService(
                monitor_type=MONITOR_TYPE, server_id=server_config.identifier
            ),
            server_config.identifier,
        )


_airflow_monitor_services_factory = AirflowServicesFactory()


def get_airflow_monitor_services_factory():
    return _airflow_monitor_services_factory
