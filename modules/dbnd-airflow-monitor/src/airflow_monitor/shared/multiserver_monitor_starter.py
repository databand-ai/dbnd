# © Copyright Databand.ai, an IBM Company 2022
import prometheus_client
import sentry_sdk

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory
from airflow_monitor.shared.multiserver import MultiServerMonitor
from dbnd._vendor import click


def start_integration_multi_server(
    monitor_config: BaseMonitorConfig,
    monitor_services_factory: MonitorServicesFactory,
    start_external_services=True,
):
    if start_external_services:
        sentry_sdk.init()
        prometheus_client.start_http_server(monitor_config.prometheus_port)

    MultiServerMonitor(
        monitor_config=monitor_config, monitor_services_factory=monitor_services_factory
    ).run()


def build_integration_monitor_config(**kwargs) -> BaseMonitorConfig:
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = BaseMonitorConfig.from_env(**monitor_config_kwargs)

    return monitor_config


def monitor_command_options(monitor_command):
    monitor_command = click.option(
        "--interval", type=click.INT, help="Interval between iterations"
    )(monitor_command)
    monitor_command = click.option(
        "--number-of-iterations",
        type=click.INT,
        help="Limit the number of periodic monitor runs",
    )(monitor_command)
    monitor_command = click.option(
        "--stop-after", type=click.INT, help="Limit time for monitor to run, in seconds"
    )(monitor_command)
    return monitor_command
