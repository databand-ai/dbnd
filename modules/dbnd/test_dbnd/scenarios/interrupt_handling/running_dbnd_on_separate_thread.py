import logging
import signal
import sys
import threading
import time

from threading import Thread

from dbnd import Task, parameter
from dbnd._core.current import try_get_databand_run
from dbnd._core.task_build.task_context import try_get_current_task


logger = logging.getLogger(__name__)

stop_requested = False


def msg(s):
    sys.stderr.write("%s:  %s\n" % (threading.get_ident(), s))
    sys.stderr.flush()


def stop():
    msg("stopping!")
    task = try_get_current_task()
    msg("Current tasks looks like: %s" % (task))

    run = try_get_databand_run()
    if run:
        run.kill()
    return


def sig_handler(signum, frame):
    msg("handling signal: %s\n" % (signum))
    msg("handling frame: %s\n" % (frame))
    #
    # global stop_requested
    # stop_requested = True
    t = Thread(target=stop)
    t.start()
    t.join()
    raise Exception()


class SleepTask(Task):
    sleep_sec = parameter.value(100)

    def run(self):
        sleep_sec = self.sleep_sec
        while sleep_sec > 0:
            time.sleep(2)
            sleep_sec -= 2
            logger.info("%d left to sleep", sleep_sec)
        return "Done sleeping"

    def on_kill(self):
        logger.info("On Kill for %s", self)


def run():
    SleepTask(task_version="now").dbnd_run()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    msg("starting\n")
    main_thread = Thread(name="dbnd_main", target=run)
    main_thread.start()
    main_thread.join()
    msg("join completed\n")
