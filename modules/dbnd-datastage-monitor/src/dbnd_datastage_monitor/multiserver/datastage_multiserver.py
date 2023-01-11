# © Copyright Databand.ai, an IBM Company 2022
import prometheus_client
import sentry_sdk

from dbnd_datastage_monitor.data.datastage_config_data import DataStageMonitorConfig
from dbnd_datastage_monitor.multiserver.datastage_services_factory import (
    DataStageMonitorServicesFactory,
)

from airflow_monitor.shared.multiserver import MultiServerMonitor
from dbnd._vendor import click


def start_datastage_multi_server_monitor(monitor_config: DataStageMonitorConfig):
    MultiServerMonitor(
        monitor_config=monitor_config,
        monitor_services_factory=DataStageMonitorServicesFactory(),
    ).run()


@click.command()
@click.option("--interval", type=click.INT, help="Interval between iterations")
@click.option(
    "--number-of-iterations",
    type=click.INT,
    help="Limit the number of periodic monitor runs",
)
@click.option(
    "--stop-after", type=click.INT, help="Limit time for monitor to run, in seconds"
)
@click.option(
    "--runner-type",
    type=click.Choice(["seq", "mp"]),
    help="Runner type. Options: seq for sequential, mp for multi-process",
)
def datastage_monitor(**kwargs):
    # noqa: E0110 pylint: disable=abstract-class-instantiated
    sentry_sdk.init(ignore_errors=["ConnectionError", "HTTPError"])
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = DataStageMonitorConfig.from_env(**monitor_config_kwargs)
    prometheus_client.start_http_server(monitor_config.prometheus_port)
    start_datastage_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    datastage_monitor()
