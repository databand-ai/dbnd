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
from dbnd_datastage_monitor.tracking_service.datastage_servers_configuration_service import (
    DataStageSyncersConfigurationService,
)
from dbnd_datastage_monitor.tracking_service.dbnd_datastage_tracking_service import (
    DbndDataStageTrackingService,
)

from airflow_monitor.shared.decorators import (
    decorate_configuration_service,
    decorate_fetcher,
    decorate_tracking_service,
)
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from dbnd._core.utils.basics.memoized import cached


MONITOR_TYPE = "datastage"


logger = logging.getLogger(__name__)


class DataStageMonitorServicesFactory(MonitorServicesFactory):
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
    def get_servers_configuration_service(self):
        return decorate_configuration_service(
            DataStageSyncersConfigurationService(
                monitor_type=MONITOR_TYPE, server_monitor_config=DataStageServerConfig
            )
        )

    @cached()
    def get_tracking_service(self, tracking_source_uid) -> DbndDataStageTrackingService:
        return decorate_tracking_service(
            DbndDataStageTrackingService(
                monitor_type=MONITOR_TYPE,
                tracking_source_uid=tracking_source_uid,
                server_monitor_config=DataStageServerConfig,
            ),
            tracking_source_uid,
        )


_datastage_services_factory = DataStageMonitorServicesFactory()


def get_datastage_services_factory():
    return _datastage_services_factory
