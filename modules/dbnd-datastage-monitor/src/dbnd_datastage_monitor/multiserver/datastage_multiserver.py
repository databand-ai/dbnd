# © Copyright Databand.ai, an IBM Company 2022

from dbnd_datastage_monitor.data.datastage_config_data import DataStageMonitorConfig
from dbnd_datastage_monitor.multiserver.datastage_services_factory import (
    DataStageMonitorServicesFactory,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import DataStageRunsSyncer

from airflow_monitor.shared.base_monitor_component_manager import (
    BaseMonitorComponentManager,
)
from airflow_monitor.shared.base_multiserver import BaseMultiServerMonitor
from dbnd._vendor import click


def start_datastage_multi_server_monitor(monitor_config: DataStageMonitorConfig):
    components_dict = {"datastage_runs_syncer": DataStageRunsSyncer}

    BaseMultiServerMonitor(
        monitor_component_manager=BaseMonitorComponentManager,
        monitor_config=monitor_config,
        components_dict=components_dict,
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
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = DataStageMonitorConfig(**monitor_config_kwargs)
    start_datastage_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    datastage_monitor()
