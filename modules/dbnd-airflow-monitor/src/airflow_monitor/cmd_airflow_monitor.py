import prometheus_client

from airflow_monitor.airflow_monitor_main import airflow_monitor_main
from airflow_monitor.config import AirflowMonitorConfig

from dbnd._vendor import click


class AirflowMonitorArgs(object):
    def __init__(
        self, since, since_now, sync_history, history_only, number_of_iterations,
    ):
        self.since = since
        self.since_now = since_now
        self.sync_history = sync_history
        self.history_only = history_only
        self.number_of_iterations = number_of_iterations


def create_airflow_monitor_config_with_values(
    interval, include_logs, include_task_args, include_xcom, sql_conn, dag_folder
):
    config = AirflowMonitorConfig()

    if interval:
        config.interval = interval
    if include_logs:
        config.include_logs = include_logs
    if include_task_args:
        config.include_task_args = include_task_args
    if include_xcom:
        config.include_xcom = include_xcom
    if sql_conn:
        config.sql_alchemy_conn = sql_conn
    if dag_folder:
        config.local_dag_folder = dag_folder

    return config


@click.command()
@click.option(
    "--interval", type=click.FLOAT, help="Sleep time (in seconds) between fetches"
)
@click.option("--include-logs", is_flag=True, help="Should include logs")
@click.option("--include-task-args", is_flag=True, help="Should include task args")
@click.option(
    "--include-xcom", is_flag=True, help="Should include task xcom dictionary"
)
@click.option("--sql-conn", type=click.STRING, help="Sql alchemy connetion string")
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
def airflow_monitor(
    interval,
    include_logs,
    include_task_args,
    include_xcom,
    sql_conn,
    dag_folder,
    since,
    sync_history,
    number_of_iterations,
    history_only,
    since_now,
):
    prometheus_client.start_http_server(8000)

    monitor_args = AirflowMonitorArgs(
        since, since_now, sync_history, history_only, number_of_iterations,
    )

    airflow_config = create_airflow_monitor_config_with_values(
        interval, include_logs, include_task_args, include_xcom, sql_conn, dag_folder,
    )

    airflow_monitor_main(monitor_args, airflow_config)
