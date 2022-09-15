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
from dbnd_datastage_monitor.fetcher.datastage_data_fetcher import DataStageDataFetcher
from dbnd_datastage_monitor.tracking_service.datastage_servers_configuration_service import (
    DataStageSyncersConfigurationService,
)
from dbnd_datastage_monitor.tracking_service.dbnd_datastage_tracking_service import (
    DbndDataStageTrackingService,
)

from airflow_monitor.shared import get_tracking_service_config_from_dbnd
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
    def get_data_fetcher(self, server_config: DataStageServerConfig):
        if server_config.number_of_fetching_threads <= 1:
            runs_getter = DataStageAssetsClient(
                client=DataStageApiHttpClient(
                    api_key=server_config.api_key,
                    project_id=server_config.project_id,
                    page_size=server_config.page_size,
                )
            )
        else:
            runs_getter = ConcurrentRunsGetter(
                client=DataStageApiHttpClient(
                    api_key=server_config.api_key,
                    project_id=server_config.project_id,
                    page_size=server_config.page_size,
                ),
                number_of_threads=server_config.number_of_fetching_threads,
            )
        fetcher = DataStageDataFetcher(
            datastage_runs_getter=runs_getter, project_id=server_config.project_id
        )

        return decorate_fetcher(fetcher, server_config.project_id)

    @cached()
    def get_servers_configuration_service(self):
        tracking_service_config = get_tracking_service_config_from_dbnd()
        return decorate_configuration_service(
            DataStageSyncersConfigurationService(
                monitor_type=MONITOR_TYPE,
                tracking_service_config=tracking_service_config,
                server_monitor_config=DataStageServerConfig,
            )
        )

    @cached()
    def get_tracking_service(self, tracking_source_uid) -> DbndDataStageTrackingService:
        tracking_service_config = get_tracking_service_config_from_dbnd()
        return decorate_tracking_service(
            DbndDataStageTrackingService(
                monitor_type=MONITOR_TYPE,
                tracking_source_uid=tracking_source_uid,
                tracking_service_config=tracking_service_config,
                server_monitor_config=DataStageServerConfig,
            ),
            tracking_source_uid,
        )


_datastage_services_factory = DataStageMonitorServicesFactory()


def get_datastage_services_factory():
    return _datastage_services_factory
