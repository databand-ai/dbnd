import logging

from multiprocessing.context import Process

from airflow_monitor.shared.error_handler import capture_monitor_exception
from airflow_monitor.shared.runners.base_runner import BaseRunner


logger = logging.getLogger(__name__)


class MultiProcessRunner(BaseRunner):
    JOIN_TIMEOUT = 60

    def __init__(self, target, **kwargs):
        super(MultiProcessRunner, self).__init__(target, **kwargs)
        self.process = None  # type: Process

    @capture_monitor_exception
    def start(self):
        self.process = Process(target=self.target, kwargs=self.kwargs)
        self.process.start()

    @capture_monitor_exception
    def stop(self):
        if self.process and self.is_alive():
            self.process.terminate()
            self.process.join(MultiProcessRunner.JOIN_TIMEOUT)
            if self.process.is_alive():
                self.process.kill()

    @capture_monitor_exception
    def heartbeat(self):
        # do we want to do something here?
        pass

    @capture_monitor_exception
    def is_alive(self):
        return self.process.is_alive()

    def __str__(self):
        s = super(MultiProcessRunner, self).__str__()
        return f"{s}({self.process})"
