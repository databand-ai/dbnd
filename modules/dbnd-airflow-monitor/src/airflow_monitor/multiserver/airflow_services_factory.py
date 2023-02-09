# © Copyright Databand.ai, an IBM Company 2022
from airflow_monitor.adapter.airflow_adapter import AirflowAdapter
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher
from airflow_monitor.fixer.runtime_fixer import AirflowRuntimeFixer
from airflow_monitor.shared.adapter.adapter import Adapter
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.decorators import (
    decorate_configuration_service,
    decorate_fetcher,
    decorate_tracking_service,
)
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)
from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.basics.memoized import cached


FETCHERS = {"db": DbFetcher, "web": WebFetcher}


MONITOR_TYPE = "airflow"


class AirflowServicesFactory(MonitorServicesFactory):
    def __init__(self, monitor_config):
        self._monitor_config = monitor_config

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
    def get_integration_management_service(self) -> IntegrationManagementService:
        return decorate_configuration_service(
            IntegrationManagementService(
                monitor_type=MONITOR_TYPE,
                server_monitor_config=AirflowServerConfig,
                integrations_name_filter=self._monitor_config.syncer_name,
            )
        )

    def get_tracking_service(
        self, server_config: BaseServerConfig
    ) -> AirflowTrackingService:
        return decorate_tracking_service(
            AirflowTrackingService(
                monitor_type=MONITOR_TYPE, server_id=server_config.identifier
            ),
            server_config.identifier,
        )

    def get_adapter(self, server_config: BaseServerConfig) -> Adapter:
        return AirflowAdapter(server_config)
