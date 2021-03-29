from airflow_monitor.common import TrackingServiceConfig
from airflow_monitor.tracking_service.af_tracking_service import (
    DbndAirflowTrackingService,
)
from airflow_monitor.tracking_service.web_tracking_service import (
    WebDbndAirflowTrackingService,
    WebServersConfigurationService,
)


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


def get_tracking_service(
    tracking_source_uid, tracking_service_config: TrackingServiceConfig = None
) -> DbndAirflowTrackingService:
    if not tracking_service_config:
        tracking_service_config = get_tracking_service_config_from_dbnd()
    return WebDbndAirflowTrackingService(
        tracking_source_uid=tracking_source_uid,
        tracking_service_config=tracking_service_config,
    )


def get_servers_configuration_service(
    tracking_service_config: TrackingServiceConfig = None,
):
    if not tracking_service_config:
        tracking_service_config = get_tracking_service_config_from_dbnd()
    return WebServersConfigurationService(
        tracking_service_config=tracking_service_config
    )
