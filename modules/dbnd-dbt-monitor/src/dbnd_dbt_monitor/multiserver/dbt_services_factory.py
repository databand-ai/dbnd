# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.syncer.dbt_runs_syncer import DbtRunsSyncer
from dbnd_dbt_monitor.tracking_service.dbt_syncer_management_service import (
    DbtSyncersManagementService,
)
from dbnd_dbt_monitor.tracking_service.dbt_tracking_service import DbtTrackingService

from airflow_monitor.shared.decorators import (
    decorate_configuration_service,
    decorate_fetcher,
    decorate_tracking_service,
)
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from dbnd._core.utils.basics.memoized import cached


MONITOR_TYPE = "dbt"

logger = logging.getLogger(__name__)


class DbtMonitorServicesFactory(MonitorServicesFactory):
    def get_components_dict(self):
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
    def get_syncer_management_service(self):
        return decorate_configuration_service(
            DbtSyncersManagementService(
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


_dbt_services_factory = DbtMonitorServicesFactory()


def get_dbt_services_factory():
    return _dbt_services_factory
