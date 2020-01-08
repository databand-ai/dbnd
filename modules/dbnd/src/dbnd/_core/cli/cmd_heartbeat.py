from dbnd._vendor import click
from dbnd._vendor.click import command


@command()
@click.option("--run-uid", required=True)
@click.option("--databand-url", required=False)
@click.option("--heartbeat-interval", required=True, type=int)
@click.option("--driver-pid", required=True, type=int)
@click.option("--tracker", required=True)
@click.option("--tracker-api", required=True)
def send_heartbeat(
    run_uid, databand_url, heartbeat_interval, driver_pid, tracker, tracker_api
):
    from dbnd import config
    from dbnd._core.settings import CoreConfig
    from dbnd._core.task_executor.heartbeat_sender import send_heartbeat_continuously

    with config(
        {
            "core": {
                "tracker": tracker.split(","),
                "tracker_api": tracker_api,
                "databand_url": databand_url,
            }
        }
    ):
        tracking_store = CoreConfig().get_tracking_store()
        send_heartbeat_continuously(
            run_uid, tracking_store, heartbeat_interval, driver_pid
        )
