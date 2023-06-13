# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.adapter.validations import run_validations
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.multiserver import start_multi_server_monitor
from dbnd import dbnd_config
from dbnd._vendor import click


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
@click.option("--syncer-name", type=click.STRING, help="Sync only specified instance")
def airflow_monitor_v2(*args, **kwargs):
    # remove all None values to not override defaults/env configured params

    # we need to load configs,
    # we have multiple get_databand_context().databand_api_client calls
    dbnd_config.load_system_configs()

    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = AirflowMonitorConfig.from_env(**monitor_config_kwargs)
    run_validations()
    start_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    airflow_monitor_v2()
