# © Copyright Databand.ai, an IBM Company 2022
import os

from airflow_monitor.shared.base_server_monitor_config import TrackingServiceConfig


def get_tracking_service_config_from_dbnd() -> TrackingServiceConfig:
    config = TrackingServiceConfig(
        url=os.getenv("DBND__CORE__DATABAND_URL"),
        access_token=os.getenv("DBND__CORE__DATABAND_ACCESS_TOKEN"),
        user=os.getenv("DBND__CORE__DBND_USER"),
        password=os.getenv("DBND__CORE__DBND_PASSWORD"),
    )
    return config
