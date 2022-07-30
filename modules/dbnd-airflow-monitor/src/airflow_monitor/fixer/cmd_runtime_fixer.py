# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.fixer.runtime_fixer import start_runtime_fixer
from dbnd._vendor import click


@click.command()
@click.argument("tracking_source_uid")
def runtime_fixer(*args, **kwargs):
    start_runtime_fixer(*args, **kwargs)


if __name__ == "__main__":
    runtime_fixer()
