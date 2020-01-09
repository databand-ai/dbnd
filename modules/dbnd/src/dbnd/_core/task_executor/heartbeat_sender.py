import contextlib
import logging
import os
import signal
import subprocess
import sys

from time import sleep, time

from dbnd._core.constants import RunState
from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd._core.utils.basics.format_exception import format_exception_as_str


logger = logging.getLogger(__name__)

TERMINATE_WAIT_TIMEOUT = 5


@contextlib.contextmanager
def start_heartbeat_sender(task_run):
    run = task_run.run
    settings = run.context.settings
    core = settings.core
    heartbeat_interval_s = settings.run.heartbeat_interval_s

    if heartbeat_interval_s > 0:
        sp = None
        heartbeat_log_fp = None
        try:
            try:
                heartbeat_log_file = task_run.log.local_heartbeat_log_file
                heartbeat_log_fp = heartbeat_log_file.open("w")
                cmd = [
                    sys.executable,
                    "-m",
                    "dbnd",
                    "send-heartbeat",
                    "--run-uid",
                    str(run.run_uid),
                    "--driver-pid",
                    str(os.getpid()),
                    "--heartbeat-interval",
                    str(heartbeat_interval_s),
                    "--tracker",
                    ",".join(core.tracker),
                    "--tracker-api",
                    core.tracker_api,
                ]
                if core.databand_url:
                    cmd += ["--databand-url", core.databand_url]
                logger.info(
                    "Starting heartbeat with log at %s using  cmd: %s",
                    heartbeat_log_file,
                    subprocess.list2cmdline(cmd),
                )
                sp = subprocess.Popen(
                    cmd, stdout=heartbeat_log_fp, stderr=subprocess.STDOUT
                )
            except Exception as ex:
                logger.info(
                    "Failed to spawn heartbeat process, you can disable it via [task]heartbeat_interval_s=0  .\n %s",
                    ex,
                )
            yield
        finally:
            if sp:
                sp.terminate()

                try:
                    sp.wait(timeout=TERMINATE_WAIT_TIMEOUT)
                except Exception:
                    logger.warning(
                        "waited %s seconds for the heartbeat sender to exit but it still hasn't exited",
                        TERMINATE_WAIT_TIMEOUT,
                    )

            if heartbeat_log_fp:
                heartbeat_log_fp.close()
    else:
        logger.info(
            "run heartbeat sender disabled (set task.heartbeat_interval_s to value > 0)"
        )
        yield


def send_heartbeat_continuously(
    run_uid, tracking_store, heartbeat_interval_s, driver_pid
):  # type: (str, TrackingStore, int, int) -> None
    logger.info(
        "starting heartbeat sender process (pid %s) with a send interval of %s seconds"
        % (os.getpid(), heartbeat_interval_s)
    )

    try:
        while True:
            loop_start = time()
            try:
                try:  # failsafe, in case the driver process died violently
                    os.getpgid(driver_pid)
                except Exception:
                    logger.info(
                        "driver process %s stopped, stopping heartbeat sender",
                        driver_pid,
                    )
                    return

                run_state = tracking_store.heartbeat(run_uid=run_uid)
                if run_state == RunState.SHUTDOWN.value:
                    logger.info("received run state SHUTDOWN: killing driver process")
                    os.kill(driver_pid, signal.SIGTERM)
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
    except Exception:
        logger.exception("Failed to run heartbeat")
    finally:
        logger.info("stopping heartbeat sender")
        sys.exit(0)
