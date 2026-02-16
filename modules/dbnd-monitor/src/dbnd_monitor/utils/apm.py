# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import os

from functools import wraps


logger = logging.getLogger(__name__)

instana_available = False
try:
    if os.environ.get("INSTANA_ENABLED", "false").lower() == "true":
        from instana.singletons import tracer
        from opentelemetry import trace

        instana_available = True
except ImportError:
    pass


def configure_apm():
    if not instana_available:
        return

    # ignore_errors = (
    #     "google.auth.exceptions:RefreshError",
    #     "requests.exceptions:ConnectionError",
    #     "requests.exceptions:HTTPError",
    #     "requests.exceptions:SSLError",
    #     "requests.exceptions:Timeout",
    #     "requests.exceptions:ConnectTimeout",
    #     "dbnd_monitor.errors:ClientConnectionError",
    #     "dbnd._core.errors.base:DatabandConnectionException",
    # )
    # apply_config_setting(
    #     global_settings(), "error_collector.ignore_classes", " ".join(ignore_errors)
    # )


@contextlib.contextmanager
def transaction_scope(name: str):
    if not instana_available:
        yield
        return

    try:
        current_span = trace.get_current_span()
        tags = {"span.kind": "entry"} if not current_span.is_recording() else {}
        active_span = tracer.start_as_current_span(name, attributes=tags)
    except Exception:
        logger.warning("Error while starting instana active_span", exc_info=True)
        active_span = None

    if not active_span:
        yield
        return

    with active_span:
        yield


def apm_track_function():
    def wrapper(f):
        if not instana_available:
            return f

        @wraps(f)
        def wrapped(*args, **kwargs):
            with transaction_scope(f"{f.__module__}.{f.__qualname__}"):
                return f(*args, **kwargs)

        return wrapped

    return wrapper
