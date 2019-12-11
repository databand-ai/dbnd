import contextlib
import logging
import os
import sys

from multiprocessing import Process
from time import sleep, time

import six

from dbnd._core.utils.basics.format_exception import format_exception_as_str


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def start_heartbeat_sender(run):
    from dbnd import config

    heartbeat_interval_s = config.getint("task", "heartbeat_interval_s")
    if heartbeat_interval_s > 0:
        process_kwargs = {
            "target": send_heartbeat,
            "args": (run.run_uid, run.context.tracking_store, heartbeat_interval_s),
        }
        if not six.PY2:
            process_kwargs["daemon"] = True

        p = Process(**process_kwargs)
        try:
            p.start()
            yield
        finally:
            p.terminate()
    else:
        logger.info(
            "run heartbeat sender disabled (set task.heartbeat_interval_s to value > 0 to enable)"
        )
        yield


def send_heartbeat(run_uid, tracking_store, heartbeat_interval_s):
    logger.info(
        "starting heartbeat sender process (pid %s) with a send interval of %s seconds"
        % (os.getpid(), heartbeat_interval_s)
    )

    parent_pid = os.getppid()
    try:
        while True:
            loop_start = time()
            try:
                try:  # failsafe, normally multiprocessing would close this process when the parent is exiting
                    os.getpgid(parent_pid)
                except ProcessLookupError:
                    return

                tracking_store.heartbeat(run_uid=run_uid)
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
