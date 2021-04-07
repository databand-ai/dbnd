import logging

import prometheus_client

from airflow_monitor.airflow_monitor_main import airflow_monitor_main
from airflow_monitor.config import AirflowMonitorConfig
from dbnd._vendor import click


logger = logging.getLogger(__name__)


class AirflowMonitorArgs(object):
    def __init__(
        self, since, since_now, sync_history, history_only, number_of_iterations,
    ):
        self.since = since
        self.since_now = since_now
        self.sync_history = sync_history
        self.history_only = history_only
        self.number_of_iterations = number_of_iterations


def override_airflow_monitor_config_with_values(
    interval, sql_conn, dag_folder, syncer_name, fetcher
):
    config = AirflowMonitorConfig()

    if interval:
        config.interval = interval
    if sql_conn:
        config.sql_alchemy_conn = sql_conn
    if dag_folder:
        config.local_dag_folder = dag_folder
    if syncer_name:
        config.syncer_name = syncer_name
    if fetcher:
        config.fetcher = fetcher

    return config


@click.command()
@click.option(
    "--prometheus-port",
    type=click.INT,
    help="The port on which to run prometheus server",
    default=8000,
)
@click.option(
    "--interval", type=click.FLOAT, help="Sleep time (in seconds) between fetches"
)
@click.option("--sql-conn", type=click.STRING, help="Sql alchemy connection string")
@click.option(
    "--dag-folder", type=click.STRING, help="Folder where the dags are stored"
)
@click.option("--since", type=click.STRING, help="Date from which to fetch")
@click.option(
    "--number-of-iterations",
    type=click.INT,
    help="Limit the number of periodic monitor runs",
)
@click.option(
    "--sync-history", is_flag=True, help="Sync history regardless of where we stopped"
)
@click.option("--history-only", is_flag=True, help="Sync only the history and exit")
@click.option("--since-now", is_flag=True, help="Start syncing from utcnow - live mode")
@click.option("--syncer-name", type=click.STRING, help="Sync only specific instance")
@click.option("--fetcher", type=click.STRING, help="Force fetcher type db/web/composer")
@click.option("--no-prometheus", is_flag=True, help="Disable prometheus metrics")
def airflow_monitor(
    prometheus_port,
    interval,
    sql_conn,
    dag_folder,
    since,
    sync_history,
    number_of_iterations,
    history_only,
    since_now,
    syncer_name,
    fetcher,
    no_prometheus,
):
    if no_prometheus:
        logger.info("Running without prometheus")
    else:
        try:
            if not prometheus_port:
                prometheus_port = AirflowMonitorConfig().prometheus_port
            prometheus_client.start_http_server(prometheus_port)
        except Exception as e:
            logger.warning(
                "Failed to start prometheus on port {}. Exception: {}".format(
                    prometheus_port, e
                )
            )

    monitor_args = AirflowMonitorArgs(
        since, since_now, sync_history, history_only, number_of_iterations,
    )

    override_airflow_monitor_config_with_values(
        interval, sql_conn, dag_folder, syncer_name, fetcher
    )

    airflow_monitor_main(monitor_args)
