from airflow_monitor.common.airflow_data import MonitorState
from airflow_monitor.shared.error_handler import (
    capture_monitor_exception as _capture_monitor_exception,
)
from airflow_monitor.tracking_service import get_servers_configuration_service


def capture_monitor_exception(message=None):
    return _capture_monitor_exception(
        message=message, configuration_service=get_servers_configuration_service(),
    )
