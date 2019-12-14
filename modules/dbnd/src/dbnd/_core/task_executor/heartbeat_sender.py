import contextlib
import logging
import os
import subprocess
import sys

from time import sleep, time

from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd.api.api_utils import ApiClient


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def start_heartbeat_sender(run):
    from dbnd import config

    heartbeat_interval_s = config.getint("task", "heartbeat_interval_s")
    if heartbeat_interval_s > 0 and config.get("core", "tracker_api") == "web":
        sp = None
        try:
            sp = subprocess.Popen(
                [
                    "dbnd",
                    "send_heartbeat",
                    "--run-uid",
                    str(run.run_uid),
                    "--tracking-url",
                    config.get("core", "tracker_url"),
                    "--heartbeat-interval",
                    heartbeat_interval_s,
                ]
            )
            yield
        finally:
            if sp:
                sp.terminate()
    else:
        logger.info(
            "run heartbeat sender disabled (set task.heartbeat_interval_s to value > 0 and core.tracker_api to web)"
        )
        yield


def send_heartbeat_continuously(run_uid, tracking_url, heartbeat_interval_s):
    logger.info(
        "starting heartbeat sender process (pid %s) with a send interval of %s seconds"
        % (os.getpid(), heartbeat_interval_s)
    )

    parent_pid = os.getppid()
    api_client = ApiClient(tracking_url)
    payload = {"run_uid": run_uid}
    try:
        while True:
            loop_start = time()
            try:
                try:  # failsafe, normally multiprocessing would close this process when the parent is exiting
                    os.getpgid(parent_pid)
                except ProcessLookupError:
                    return

                api_client.api_request("heartbeat", payload)
            except KeyboardInterrupt:
                logger.info("stopping heartbeat sender process due to interrupt")
                return
            except Exception:
                logger.error("failed to send heartbeat: %s", format_exception_as_str())

            time_to_sleep_s = max(0, time() + heartbeat_interval_s - loop_start)
            if time_to_sleep_s > 0:
                sleep(time_to_sleep_s)
    except KeyboardInterrupt:
        return
    finally:
        logger.info("stopping heartbeat sender")
        sys.exit(0)
