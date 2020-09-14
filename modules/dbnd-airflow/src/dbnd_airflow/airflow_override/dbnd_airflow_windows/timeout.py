import threading

from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.log.logging_mixin import LoggingMixin


class timeout(LoggingMixin):
    """
    To be used in a ``with`` block and timeout its content.
    """

    def __init__(self, seconds=1, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message
        self.timer = threading.Timer(seconds, self.handle_timeout)

    def handle_timeout(self, signum, frame):
        self.log.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            self.timer.start()
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            self.timer.cancel()
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)
