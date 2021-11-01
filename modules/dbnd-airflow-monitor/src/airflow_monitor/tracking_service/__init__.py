from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_methods,
    measure_time,
)
from airflow_monitor.shared import get_tracking_service_config_from_dbnd
from airflow_monitor.shared.base_tracking_service import (
    BaseDbndTrackingService,
    WebServersConfigurationService,
)
from airflow_monitor.tracking_service.web_tracking_service import (
    AirflowDbndTrackingService,
)
from dbnd._core.utils.basics.memoized import cached
from dbnd._vendor.tenacity import retry, stop_after_attempt


MONITOR_TYPE = "airflow"


@cached()
def get_tracking_service(tracking_source_uid) -> AirflowDbndTrackingService:
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_tracking_service(
        AirflowDbndTrackingService(
            monitor_type=MONITOR_TYPE,
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=tracking_service_config,
            server_monitor_config=AirflowServerConfig,
        ),
        tracking_source_uid,
    )


def decorate_tracking_service(tracking_service, label):
    return decorate_methods(
        tracking_service,
        BaseDbndTrackingService,
        measure_time(METRIC_REPORTER.dbnd_api_response_time, label),
        retry(stop=stop_after_attempt(2), reraise=True),
    )


@cached()
def get_servers_configuration_service():
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_configuration_service(
        WebServersConfigurationService(
            monitor_type=MONITOR_TYPE,
            tracking_service_config=tracking_service_config,
            server_monitor_config=AirflowServerConfig,
        )
    )


def decorate_configuration_service(configuration_service):
    return decorate_methods(
        configuration_service,
        WebServersConfigurationService,
        measure_time(METRIC_REPORTER.dbnd_api_response_time, "global"),
        retry(stop=stop_after_attempt(2), reraise=True),
    )
