import logging
import traceback

from datetime import timedelta
from functools import wraps

from airflow_monitor.common.airflow_data import MonitorState
from airflow_monitor.tracking_service import (
    DbndAirflowTrackingService,
    get_servers_configuration_service,
)
from dbnd._core.errors import DatabandError
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.cachetools import TTLCache, cached


logger = logging.getLogger(__name__)


def capture_monitor_exception(message=None):
    def wrapper(f):
        @wraps(f)
        def wrapped(obj, *args, **kwargs):
            obj_logger = getattr(obj.__module__, "logger", None) or logging.getLogger(
                obj.__module__
            )
            try:
                obj_logger.debug("[%s] %s", obj, message or f.__name__)
                result = f(obj, *args, **kwargs)

                _report_error(obj, f, None)

                return result
            except Exception as e:
                obj_logger.exception("[%s] Error during %s", obj, message or f.__name__)

                err_message = traceback.format_exc()
                _log_exception_to_server(err_message)

                if isinstance(e, DatabandError):
                    err_message = logger_format_for_databand_error(e)

                err_message += "\nTimestamp: {}".format(utcnow())

                _report_error(obj, f, err_message)

        return wrapped

    if callable(message):
        # probably was used as @capture_monitor_exception
        func, message = message, None
        return wrapper(func)

    return wrapper


cache = TTLCache(maxsize=5, ttl=timedelta(hours=1).total_seconds())


# cached in order to avoid logging same messages over and over again
@cached(cache)
def _log_exception_to_server(exception_message: str):
    client = get_servers_configuration_service()
    if client is None:
        return
    try:
        client.report_exception(exception_message)
    except Exception as e:
        logger.warning("Error sending monitoring exception message")


def _get_tracking_service(obj):
    return getattr(obj, "tracking_service", None)  # type: DbndAirflowTrackingService


def _report_error(obj, f, err_message):
    tracking_service = _get_tracking_service(obj)
    if tracking_service is None:
        return
    try:
        tracking_service.report_error(f, err_message)
    except Exception as e:
        logger.warning("Error sending error message", exc_info=True)
