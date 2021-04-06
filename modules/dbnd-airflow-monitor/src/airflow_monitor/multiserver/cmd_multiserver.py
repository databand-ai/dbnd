from airflow_monitor.multiserver.multiserver import start_multi_server_monitor
from dbnd._vendor import click


@click.command()
@click.option(
    "--interval", default=10, type=click.INT, help="Interval between iterations"
)
@click.option(
    "--runner-type",
    default="seq",
    type=click.Choice(["seq", "mp"]),
    help="Runner type. Options: seq for sequential, mp for multi-process",
)
@click.option(
    "--number-of-iterations",
    type=click.INT,
    help="Limit the number of periodic monitor runs",
)
@click.argument("syncer_name", type=click.STRING, default=None, nargs=-1)
def multi_server_syncer(*args, **kwargs):
    start_multi_server_monitor(*args, **kwargs)


if __name__ == "__main__":
    multi_server_syncer()
