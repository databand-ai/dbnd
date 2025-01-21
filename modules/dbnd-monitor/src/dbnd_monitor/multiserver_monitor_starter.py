# © Copyright Databand.ai, an IBM Company 2022
from typing import Type

import prometheus_client

from dbnd._vendor import click
from dbnd_monitor.base_integration import BaseIntegration
from dbnd_monitor.base_monitor_config import BaseMonitorConfig
from dbnd_monitor.integration_management_service import IntegrationManagementService
from dbnd_monitor.multiserver import MultiServerMonitor
from dbnd_monitor.utils.apm import configure_apm


def start_integration_multi_server(
    monitor_config: BaseMonitorConfig,
    integration: Type[BaseIntegration],
    start_external_services=True,
):
    if start_external_services:
        prometheus_client.start_http_server(monitor_config.prometheus_port)
        configure_apm()

    MultiServerMonitor(
        monitor_config=monitor_config,
        integration_management_service=IntegrationManagementService(),
        integration_types=[integration],
    ).run()


@click.command()
@click.pass_obj
@click.option("--interval", type=click.INT, help="Interval between iterations")
@click.option(
    "--number-of-iterations",
    type=click.INT,
    help="Limit the number of periodic monitor runs",
)
@click.option(
    "--stop-after", type=click.INT, help="Limit time for monitor to run, in seconds"
)
@click.option("--syncer-name", type=click.STRING, help="Sync only specified instance")
def monitor_cmd(obj, **kwargs):
    # remove all None values to not override defaults/env configured params
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = BaseMonitorConfig.from_env(**monitor_config_kwargs)

    start_integration_multi_server(monitor_config, obj)
