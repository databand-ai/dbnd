import logging

from multiprocessing.context import Process

from airflow_monitor.multiserver.runners.base_runner import BaseRunner


logger = logging.getLogger(__name__)


class MultiProcessRunner(BaseRunner):
    JOIN_TIMEOUT = 60

    def __init__(self, target, **kwargs):
        super(MultiProcessRunner, self).__init__(target, **kwargs)
        self.process = None  # type: Process

    def start(self):
        self.process = Process(target=self.target, kwargs=self.kwargs)
        self.process.start()

    def stop(self):
        if self.process and self.is_alive():
            self.process.terminate()
            self.process.join(MultiProcessRunner.JOIN_TIMEOUT)
            if self.process.is_alive():
                self.process.kill()

    def heartbeat(self):
        # do we want to do something here?
        pass

    def is_alive(self):
        return self.process.is_alive()
