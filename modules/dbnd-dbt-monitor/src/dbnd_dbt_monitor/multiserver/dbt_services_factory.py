# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.syncer.dbt_runs_syncer import DbtRunsSyncer
from dbnd_dbt_monitor.tracking_service.dbt_tracking_service import DbtTrackingService

from airflow_monitor.shared.decorators import (
    decorate_configuration_service,
    decorate_fetcher,
    decorate_tracking_service,
)
from airflow_monitor.shared.generic_syncer import GenericSyncer
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from dbnd._core.utils.basics.memoized import cached


MONITOR_TYPE = "dbt"

logger = logging.getLogger(__name__)


class DbtMonitorServicesFactory(MonitorServicesFactory):
    def get_components_dict(self, is_generic_syncer_enabled=False):
        if is_generic_syncer_enabled:
            return {"generic_syncer": GenericSyncer}
        return {"dbt_runs_syncer": DbtRunsSyncer}

    def get_data_fetcher(self, server_config: DbtServerConfig):
        fetcher = DbtCloudDataFetcher.create_from_dbt_credentials(
            dbt_cloud_api_token=server_config.api_token,
            dbt_cloud_account_id=server_config.account_id,
            batch_size=server_config.runs_bulk_size,
            job_id=server_config.job_id,
        )
        return decorate_fetcher(fetcher, server_config.account_id)

    @cached()
    def get_integration_management_service(self):
        return decorate_configuration_service(
            IntegrationManagementService(
                monitor_type=MONITOR_TYPE, server_monitor_config=DbtServerConfig
            )
        )

    def get_tracking_service(self, server_config) -> DbtTrackingService:
        return decorate_tracking_service(
            DbtTrackingService(
                monitor_type=MONITOR_TYPE, server_id=server_config.identifier
            ),
            server_config.identifier,
        )

    def get_components(
        self,
        integration_config: DbtServerConfig,
        integration_management_service: IntegrationManagementService,
    ) -> list:
        # TODO: migrate is_generic_syncer_enabled_flag
        integration_config.is_generic_syncer_enabled = True
        if not integration_config.is_generic_syncer_enabled:
            return super().get_components(
                integration_config, integration_management_service
            )

        tracking_service = self.get_tracking_service(integration_config)
        data_fetcher = self.get_data_fetcher(integration_config)
        components_dict = self.get_components_dict(is_generic_syncer_enabled=True)

        all_components = []
        for _, syncer_class in components_dict.items():
            from dbnd_dbt_monitor.adapter.dbt_adapter import DbtAdapter

            syncer_instance = syncer_class(
                config=integration_config,
                tracking_service=tracking_service,
                integration_management_service=integration_management_service,
                adapter=DbtAdapter(data_fetcher=data_fetcher),
                syncer_instance_id=str(integration_config.account_id),
            )
            all_components.append(syncer_instance)
        return all_components
