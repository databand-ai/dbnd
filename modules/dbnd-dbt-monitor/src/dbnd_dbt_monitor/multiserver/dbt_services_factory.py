# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)
from dbnd_dbt_monitor.tracking_service.dbt_servers_configuration_service import (
    DbtSyncersConfigurationService,
)

from airflow_monitor.shared import get_tracking_service_config_from_dbnd
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
    def get_data_fetcher(self, server_config: DbtServerConfig):
        fetcher = DbtCloudDataFetcher.create_from_dbt_credentials(
            dbt_cloud_api_token=server_config.api_token,
            dbt_cloud_account_id=server_config.account_id,
            batch_size=server_config.runs_bulk_size,
            job_id=server_config.job_id,
        )
        return decorate_fetcher(fetcher, server_config.account_id)

    @cached()
    def get_servers_configuration_service(self):
        tracking_service_config = get_tracking_service_config_from_dbnd()
        return decorate_configuration_service(
            DbtSyncersConfigurationService(
                monitor_type=MONITOR_TYPE,
                tracking_service_config=tracking_service_config,
                server_monitor_config=DbtServerConfig,
            )
        )

    @cached()
    def get_tracking_service(self, tracking_source_uid) -> DbndDbtTrackingService:
        tracking_service_config = get_tracking_service_config_from_dbnd()
        return decorate_tracking_service(
            DbndDbtTrackingService(
                monitor_type=MONITOR_TYPE,
                tracking_source_uid=tracking_source_uid,
                tracking_service_config=tracking_service_config,
                server_monitor_config=DbtServerConfig,
            ),
            tracking_source_uid,
        )


_dbt_services_factory = DbtMonitorServicesFactory()


def get_dbt_services_factory():
    return _dbt_services_factory
