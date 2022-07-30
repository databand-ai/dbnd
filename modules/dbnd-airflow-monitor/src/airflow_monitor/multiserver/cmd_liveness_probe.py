# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.shared.liveness_probe import (
    MAX_TIME_DIFF_IN_SECONDS,
    check_monitor_alive,
)
from dbnd._vendor import click


@click.command()
@click.option(
    "--max-time-diff", type=click.INT, help="Maximum time from last liveness file"
)
def airflow_monitor_v2_alive(max_time_diff):
    check_monitor_alive(
        monitor_type="airflow", max_time_diff=max_time_diff or MAX_TIME_DIFF_IN_SECONDS
    )


if __name__ == "__main__":
    airflow_monitor_v2_alive()
