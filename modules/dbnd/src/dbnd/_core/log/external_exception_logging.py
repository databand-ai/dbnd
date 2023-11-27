# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys
import traceback

from functools import wraps

import dbnd

from dbnd._core.log import dbnd_log_exception
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


def _format_exception(e_type, e_value, e_traceback):
    return "".join(traceback.format_exception(e_type, value=e_value, tb=e_traceback))


def log_exception_to_server(exception=None, source="tracking-sdk"):
    try:
        from dbnd._core.current import try_get_databand_context

        databand_context = try_get_databand_context()
        if not databand_context:
            return

        client = databand_context.databand_api_client
        if client is None or not client.is_configured():
            return

        e_type, e_value, e_traceback = sys.exc_info()
        if exception:
            e_type, e_value, e_traceback = (
                type(exception),
                exception,
                exception.__traceback__,
            )

        trace = _format_exception(e_type, e_value, e_traceback)

        data = {
            "dbnd_version": dbnd.__version__,
            "source": source,
            "stack_trace": trace,
            "timestamp": utcnow().isoformat(),
        }
        return client.api_request(endpoint="log_exception", method="POST", data=data)
    except Exception:  # noqa
        dbnd_log_exception("Error sending monitoring exception message", exc_info=True)


def capture_tracking_exception(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            log_exception_to_server(e)
            raise e

    return wrapper
