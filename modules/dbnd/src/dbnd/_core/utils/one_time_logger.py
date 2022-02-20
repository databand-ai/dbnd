import logging

from dbnd._core.utils.basics.singleton_context import SingletonContext


logger = logging.getLogger(__name__)


class OneTimeLogger(SingletonContext):
    """
    This class helps log messages for different types and makes sure that every type will be logged only once
    """

    def __init__(self):
        self._log_hostory = set()

    def log_once(self, message, message_type, log_level):
        if message_type not in self._log_hostory:
            logger.log(level=log_level, msg=message)
            self._log_hostory.add(message_type)


_OneTimeLogger = OneTimeLogger.try_instance()


def get_one_time_logger():
    return OneTimeLogger.get_instance()
