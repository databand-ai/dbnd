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
    #     "airflow_monitor.shared.errors:ClientConnectionError",
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
        tags = {"span.kind": "entry"} if not tracer.active_span else {}
        active_span = tracer.start_active_span(name, tags=tags)
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
