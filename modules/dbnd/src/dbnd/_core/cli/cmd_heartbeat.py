from dbnd._core.task_executor.heartbeat_sender import send_heartbeat_continuously
from dbnd._vendor import click
from dbnd._vendor.click import command


@command()
@click.option("--run-uid", required=True)
@click.option("--tracking-url", required=True)
@click.option("--heartbeat-interval", required=True, type=int)
@click.option("--driver-pid", required=True, type=int)
def send_heartbeat(run_uid, tracking_url, heartbeat_interval, driver_pid):
    send_heartbeat_continuously(run_uid, tracking_url, heartbeat_interval, driver_pid)
