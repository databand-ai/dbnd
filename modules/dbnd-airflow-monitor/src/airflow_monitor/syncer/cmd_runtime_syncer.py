from airflow_monitor.syncer.runtime_syncer import start_runtime_syncer
from dbnd._vendor import click


@click.command()
@click.argument("tracking_source_uid")
def runtime_syncer(*args, **kwargs):
    start_runtime_syncer(*args, **kwargs)


if __name__ == "__main__":
    runtime_syncer()
