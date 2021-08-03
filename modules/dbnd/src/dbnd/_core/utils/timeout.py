import logging
import signal
import time

from contextlib import contextmanager


logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


@contextmanager
def timeout(seconds, handler=None):
    def timeout_handler(signum, frame):
        raise TimeoutError()

    handler = handler or timeout_handler
    original_handler = signal.signal(signal.SIGALRM, timeout_handler)

    try:
        original_alarm = signal.alarm(seconds)
        if 0 < original_alarm < seconds:  # existing alarm is going to fire earlier
            signal.alarm(original_alarm)
        start_time = time.time()
        yield
    finally:
        if original_alarm:
            original_alarm -= round(time.time() - start_time)
        signal.alarm(original_alarm)
        signal.signal(signal.SIGALRM, original_handler)


def wait_until(predicate, wait_timeout, period=1):
    try:
        with timeout(wait_timeout, handler=lambda *args: None):
            start_time = time.time()
            while not predicate():
                time.sleep(period)
                logger.debug(round(time.time() - start_time))
            return round(time.time() - start_time) or 1
    except TimeoutError:
        pass
    return False
