# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.multiserver.airflow_services_factory import (
    get_airflow_monitor_services_factory,
)
from airflow_monitor.shared.error_handler import CaptureMonitorExceptionDecorator


capture_monitor_exception = CaptureMonitorExceptionDecorator(
    configuration_service_provider=get_airflow_monitor_services_factory().get_servers_configuration_service
)
