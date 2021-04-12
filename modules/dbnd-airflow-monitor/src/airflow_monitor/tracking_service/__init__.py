from airflow_monitor.common.config_data import TrackingServiceConfig
from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_measure_time,
)
from airflow_monitor.tracking_service.af_tracking_service import (
    DbndAirflowTrackingService,
    ServersConfigurationService,
)
from airflow_monitor.tracking_service.web_tracking_service import (
    WebDbndAirflowTrackingService,
    WebServersConfigurationService,
)
from dbnd._core.utils.basics.memoized import cached


def get_tracking_service_config_from_dbnd() -> TrackingServiceConfig:
    from dbnd import get_databand_context

    conf = get_databand_context().settings.core
    config = TrackingServiceConfig(
        url=conf.databand_url,
        access_token=conf.databand_access_token,
        user=conf.dbnd_user,
        password=conf.dbnd_password,
    )
    return config


@cached()
def get_tracking_service(tracking_source_uid) -> DbndAirflowTrackingService:
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_tracking_service(
        WebDbndAirflowTrackingService(
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=tracking_service_config,
        ),
        tracking_source_uid,
    )


def decorate_tracking_service(tracking_service, label):
    return decorate_measure_time(
        tracking_service,
        DbndAirflowTrackingService,
        METRIC_REPORTER.dbnd_api_response_time,
        label,
    )


def get_servers_configuration_service():
    tracking_service_config = get_tracking_service_config_from_dbnd()
    return decorate_configuration_service(
        WebServersConfigurationService(tracking_service_config=tracking_service_config)
    )


def decorate_configuration_service(configuration_service):
    return decorate_measure_time(
        configuration_service,
        ServersConfigurationService,
        METRIC_REPORTER.dbnd_api_response_time,
        "global",
    )
