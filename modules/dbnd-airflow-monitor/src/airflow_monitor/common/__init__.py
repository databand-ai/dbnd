import logging
import traceback

from functools import wraps

from airflow_monitor.common.airflow_data import MonitorState
from airflow_monitor.tracking_service import DbndAirflowTrackingService
from dbnd._core.errors import DatabandError
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error
from dbnd._core.utils.timezone import utcnow


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

                report_error(obj, f, None)

                return result
            except Exception as e:
                obj_logger.exception("[%s] Error during %s", obj, message or f.__name__)

                if isinstance(e, DatabandError):
                    err_message = logger_format_for_databand_error(e)
                else:
                    err_message = traceback.format_exc()
                err_message += "\nTimestamp: {}".format(utcnow())

                report_error(obj, f, err_message)

        return wrapped

    if callable(message):
        # probably was used as @capture_monitor_exception
        func, message = message, None
        return wrapper(func)

    return wrapper


def report_error(obj, f, err_message):
    tracking_service = getattr(
        obj, "tracking_service", None
    )  # type: DbndAirflowTrackingService
    if tracking_service is not None:
        try:
            tracking_service.report_error(f, err_message)
        except Exception as e:
            logger.warning("Error sending error message", exc_info=True)
