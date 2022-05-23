from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)
from dbnd_dbt_monitor.tracking_service.dbt_servers_configuration_service import (
    DbtSyncersConfigurationService,
)

from airflow_monitor.common.metric_reporter import decorate_methods
from airflow_monitor.shared import get_tracking_service_config_from_dbnd
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from dbnd._core.utils.basics.memoized import cached
from dbnd._vendor.tenacity import retry, stop_after_attempt


MONITOR_TYPE = "dbt"


@cached()
def get_tracking_service(tracking_source_uid) -> DbndDbtTrackingService:
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_tracking_service(
        DbndDbtTrackingService(
            monitor_type=MONITOR_TYPE,
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=tracking_service_config,
            server_monitor_config=DbtServerConfig,
        )
    )


def decorate_tracking_service(tracking_service: BaseDbndTrackingService):
    return decorate_methods(
        tracking_service,
        DbndDbtTrackingService,
        retry(stop=stop_after_attempt(2), reraise=True),
    )


@cached()
def get_servers_configuration_service():
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_configuration_service(
        DbtSyncersConfigurationService(
            monitor_type=MONITOR_TYPE,
            tracking_service_config=tracking_service_config,
            server_monitor_config=DbtServerConfig,
        )
    )


def decorate_configuration_service(configuration_service):
    return decorate_methods(
        configuration_service,
        DbtSyncersConfigurationService,
        retry(stop=stop_after_attempt(2), reraise=True),
    )
