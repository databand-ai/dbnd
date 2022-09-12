# © Copyright Databand.ai, an IBM Company 2022

from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorConfig
from dbnd_dbt_monitor.multiserver.dbt_services_factory import get_dbt_services_factory
from dbnd_dbt_monitor.syncer.dbt_runs_syncer import DbtRunsSyncer

from airflow_monitor.shared.base_monitor_component_manager import (
    BaseMonitorComponentManager,
)
from airflow_monitor.shared.base_multiserver import BaseMultiServerMonitor
from dbnd._vendor import click


def start_dbt_multi_server_monitor(monitor_config: DbtMonitorConfig):
    components_dict = {"dbt_runs_syncer": DbtRunsSyncer}

    BaseMultiServerMonitor(
        monitor_component_manager=BaseMonitorComponentManager,
        monitor_config=monitor_config,
        components_dict=components_dict,
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
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = DbtMonitorConfig(**monitor_config_kwargs)
    start_dbt_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    dbt_monitor()
