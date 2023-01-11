# © Copyright Databand.ai, an IBM Company 2022
import sentry_sdk

from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorConfig
from dbnd_dbt_monitor.multiserver.dbt_services_factory import get_dbt_services_factory

from airflow_monitor.shared.multiserver import MultiServerMonitor
from dbnd._vendor import click


def start_dbt_multi_server_monitor(monitor_config: DbtMonitorConfig):
    MultiServerMonitor(
        monitor_config=monitor_config,
        monitor_services_factory=get_dbt_services_factory(),
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
def dbt_monitor(**kwargs):
    # noqa: E0110 pylint: disable=abstract-class-instantiated
    sentry_sdk.init()
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = DbtMonitorConfig.from_env(**monitor_config_kwargs)
    start_dbt_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    dbt_monitor()
