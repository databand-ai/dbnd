# © Copyright Databand.ai, an IBM Company 2022

import logging
import signal
import time

# handle SIGINT from SyncManager object
from multiprocessing.managers import SyncManager


def mgr_sig_handler(signal, frame):
    print("not closing the mgr")


# initilizer for SyncManager
def mgr_init():
    signal.signal(signal.SIGINT, mgr_sig_handler)
    # signal.signal(signal.SIGINT, signal.SIG_IGN) # <- OR do this to just ignore the signal
    print("initialized mananger")


class ClassWithManager(object):
    def __init__(self):
        self._manager = SyncManager()
        self._manager.start(mgr_init)
        self.task_queue = self._manager.Queue()

    def run_and_sleep(self, sleep_time):
        try:

            time.sleep(sleep_time)
        finally:
            self.end()

    def end(self):

        logging.error(
            "Executor shutting down, task_queue approximate size=%d",
            self.task_queue.qsize(),
        )


if __name__ == "__main__":
    c = ClassWithManager()
    c.run_and_sleep(100)
