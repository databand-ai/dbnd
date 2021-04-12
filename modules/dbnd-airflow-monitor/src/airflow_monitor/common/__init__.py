import logging
import traceback

from functools import wraps

from airflow_monitor.common.airflow_data import MonitorState
from airflow_monitor.tracking_service import DbndAirflowTrackingService


def capture_monitor_exception(message=None):
    def wrapper(f):
        @wraps(f)
        def wrapped(obj, *args, **kwargs):
            logger = getattr(obj.__module__, "logger", None) or logging.getLogger(
                obj.__module__
            )
            try:
                logger.debug("[%s] %s", obj, message or f.__name__)
                return f(obj, *args, **kwargs)
            except Exception:
                logger.exception("[%s] Error during %s", obj, message or f.__name__)
                tracking_service = getattr(
                    obj, "tracking_service", None
                )  # type: DbndAirflowTrackingService
                if tracking_service:
                    tracking_service.update_monitor_state(
                        MonitorState(monitor_error_message=traceback.format_exc())
                    )

                # return default_return() if callable(default_return) else default_return

        return wrapped

    if callable(message):
        # probably was used as @capture_monitor_exception
        return wrapper(message)

    return wrapper
