from dbnd_dbt_monitor.data.dbt_config_data import DbtMonitorConfig
from dbnd_dbt_monitor.multiserver.dbt_multiserver import start_dbt_multi_server_monitor

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
def dbt_monitor(**kwargs):
    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = DbtMonitorConfig(**monitor_config_kwargs)
    start_dbt_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    dbt_monitor()
