# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_api_client import (
    DataStageApiHttpClient,
)
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    ConcurrentRunsGetter,
    DataStageAssetsClient,
)
from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import DataStageRunsSyncer
from dbnd_datastage_monitor.tracking_service.datastage_tracking_service import (
    DataStageTrackingService,
)

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


MONITOR_TYPE = "datastage"

logger = logging.getLogger(__name__)


class DataStageMonitorServicesFactory(MonitorServicesFactory):
    def get_components_dict(self, is_generic_syncer_enabled=True):
        if is_generic_syncer_enabled:
            return {"generic_syncer": GenericSyncer}
        # turn off generic syncer by default
        return {"datastage_runs_syncer": DataStageRunsSyncer}

    @staticmethod
    def get_asset_clients(server_config: DataStageServerConfig):
        if server_config.number_of_fetching_threads <= 1:
            asset_clients = [
                DataStageAssetsClient(
                    client=DataStageApiHttpClient(
                        host_name=server_config.host_name
                        or DataStageApiHttpClient.DEFAULT_API_HOST,
                        authentication_provider_url=server_config.authentication_provider_url,
                        authentication_type=server_config.authentication_type,
                        api_key=server_config.api_key,
                        project_id=project_id,
                        page_size=server_config.page_size,
                        log_exception_to_webserver=server_config.log_exception_to_webserver,
                    )
                )
                for project_id in server_config.project_ids
            ]
        else:
            asset_clients = [
                ConcurrentRunsGetter(
                    client=DataStageApiHttpClient(
                        host_name=server_config.host_name,
                        authentication_provider_url=server_config.authentication_provider_url,
                        authentication_type=server_config.authentication_type,
                        api_key=server_config.api_key,
                        project_id=project_id,
                        page_size=server_config.page_size,
                        log_exception_to_webserver=server_config.log_exception_to_webserver,
                    ),
                    number_of_threads=server_config.number_of_fetching_threads,
                )
                for project_id in server_config.project_ids
            ]

        return asset_clients

    def get_data_fetcher(self, server_config: DataStageServerConfig):
        asset_clients = self.get_asset_clients(server_config)
        fetcher = MultiProjectDataStageDataFetcher(
            datastage_project_clients=asset_clients
        )

        return decorate_fetcher(fetcher, server_config.source_name)

    @cached()
    def get_integration_management_service(self):
        return decorate_configuration_service(
            IntegrationManagementService(
                monitor_type=MONITOR_TYPE, server_monitor_config=DataStageServerConfig
            )
        )

    def get_tracking_service(self, server_config) -> DataStageTrackingService:
        return decorate_tracking_service(
            DataStageTrackingService(
                monitor_type=MONITOR_TYPE, server_id=server_config.identifier
            ),
            server_config.identifier,
        )

    def get_adapter(self, server_config):
        from dbnd_datastage_monitor.adapter.datastage_adapter import DataStageAdapter

        datastage_asset_clients = self.get_asset_clients(server_config)
        return DataStageAdapter(
            config=server_config, datastage_assets_client=datastage_asset_clients[0]
        )

    def get_components(
        self,
        integration_config: DataStageServerConfig,
        integration_management_service: IntegrationManagementService,
    ) -> list:
        # turn off generic syncer by default
        if not integration_config.is_generic_syncer_enabled:
            logger.info("running legacy datastage syncer")
            return super().get_components(
                integration_config, integration_management_service
            )
        logger.info("running new datastage syncer")
        tracking_service = self.get_tracking_service(integration_config)
        self.get_data_fetcher(integration_config)
        components_dict = self.get_components_dict(is_generic_syncer_enabled=True)

        all_components = []
        for _, syncer_class in components_dict.items():
            datastage_asset_clients = self.get_asset_clients(integration_config)
            for datastage_asset_client in datastage_asset_clients:
                from dbnd_datastage_monitor.adapter.datastage_adapter import (
                    DataStageAdapter,
                )

                syncer_instance = syncer_class(
                    config=integration_config,
                    tracking_service=tracking_service,
                    integration_management_service=integration_management_service,
                    adapter=DataStageAdapter(
                        config=integration_config,
                        datastage_assets_client=datastage_asset_client,
                    ),
                    syncer_instance_id=datastage_asset_client.project_id,
                )
                all_components.append(syncer_instance)

            return all_components
