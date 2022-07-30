# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.shared.base_server_monitor_config import TrackingServiceConfig


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
