from dbnd._vendor import click
from dbnd._vendor.click import command
from dbnd._vendor.click.types import IntParamType


@command()
@click.option("--run-uid", required=True)
@click.option("--tracking-url", required=True)
@click.option("--heartbeat-interval", required=True, type=IntParamType())
@click.option("--tracker", required=True)
@click.option("--tracker-api", required=True)
def send_heartbeat(run_uid, tracking_url, heartbeat_interval, tracker, tracker_api):
    from dbnd import config
    from dbnd._core.settings import CoreConfig
    from dbnd._core.task_executor.heartbeat_sender import send_heartbeat_continuously

    with config(
        {
            "core": {
                "tracker": tracker.split(","),
                "tracker_api": tracker_api,
                "tracking_url": tracking_url,
            }
        }
    ):
        tracking_store = CoreConfig().get_tracking_store()
        send_heartbeat_continuously(run_uid, tracking_store, heartbeat_interval)
