import logging
import traceback

from functools import wraps

from airflow_monitor.common.airflow_data import MonitorState
from airflow_monitor.tracking_service import DbndAirflowTrackingService
from dbnd._core.errors import DatabandError
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error
from dbnd._core.utils.timezone import utcnow


ERROR_REPORTED_KEY = "__error_reported__"


def capture_monitor_exception(message=None):
    def wrapper(f):
        @wraps(f)
        def wrapped(obj, *args, **kwargs):
            logger = getattr(obj.__module__, "logger", None) or logging.getLogger(
                obj.__module__
            )
            error_captured = False
            try:
                logger.debug("[%s] %s", obj, message or f.__name__)
                return f(obj, *args, **kwargs)
            except Exception as e:
                logger.exception("[%s] Error during %s", obj, message or f.__name__)
                error_captured = True

                if isinstance(e, DatabandError):
                    err_message = logger_format_for_databand_error(e)
                else:
                    err_message = traceback.format_exc()
                err_message += "\nTimestamp: {}".format(utcnow())

                setattr(f, ERROR_REPORTED_KEY, True)
                _report_error(obj, err_message)
            finally:
                # clean if this iteration was ok but last iteration we've reported error
                if not error_captured and getattr(f, ERROR_REPORTED_KEY, False):
                    setattr(f, ERROR_REPORTED_KEY, False)
                    _report_error(obj, None)

        return wrapped

    if callable(message):
        # probably was used as @capture_monitor_exception
        return wrapper(message)

    return wrapper


def _report_error(obj, err_message):
    tracking_service = getattr(
        obj, "tracking_service", None
    )  # type: DbndAirflowTrackingService
    if tracking_service:
        tracking_service.update_monitor_state(
            MonitorState(monitor_error_message=err_message)
        )
