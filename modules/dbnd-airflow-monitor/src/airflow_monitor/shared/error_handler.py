# © Copyright Databand.ai, an IBM Company 2022

import logging
import traceback

from contextlib import contextmanager
from datetime import timedelta
from typing import Optional
from uuid import UUID

from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.errors import ClientConnectionError
from airflow_monitor.shared.reporting_service import ReportingService
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.cachetools import TTLCache, cached


logger = logging.getLogger(__name__)
ERRORS_TO_IGNORE = [ClientConnectionError]


@contextmanager
def capture_component_exception(component: BaseComponent, function_name: str):
    syncer_logger = getattr(component.__module__, "logger", None) or logging.getLogger(
        component.__module__
    )
    class_name = component.__class__.__name__
    full_function_name = f"{class_name}.{function_name}"

    syncer_logger.debug("Running function %s from %s", function_name, component)

    try:
        yield
        # No errors, send error None to clean the existing error if any
        _report_error(
            component.reporting_service, component.config.uid, full_function_name, None
        )

        component.report_sync_metrics(is_success=True)

    except Exception as exc:
        syncer_logger.exception(
            "Error when running function %s from %s, integration_uid: %s, tracking_source_uid: %s",
            function_name,
            class_name,
            component.config.uid,
            str(component.config.tracking_source_uid),
        )

        err_message = traceback.format_exc()
        if not should_ignore_error(exc):
            _log_exception_to_server(err_message, component.reporting_service)

        err_message += f"\nTimestamp: {utcnow()}"

        _report_error(
            component.reporting_service,
            component.config.uid,
            full_function_name,
            err_message,
        )

        component.report_sync_metrics(is_success=False)


log_exception_cache = TTLCache(maxsize=5, ttl=timedelta(hours=1).total_seconds())


# cached in order to avoid logging same messages over and over again
@cached(log_exception_cache)
def _log_exception_to_server(
    exception_message: str, reporting_service: ReportingService
):
    try:
        reporting_service.report_exception_to_web_server(exception_message)
    except Exception:
        logger.warning("Error sending monitoring exception message")


def _report_error(
    reporting_service: ReportingService,
    syncer_id: UUID,
    function_name: str,
    err_message: Optional[str],
):
    try:
        reporting_service.report_error(syncer_id, function_name, err_message)
    except Exception:
        logger.warning("Error sending error message", exc_info=True)


def should_ignore_error(exc: Exception) -> bool:
    """returns whether an error should be logged to server"""
    return any([isinstance(exc, error) for error in ERRORS_TO_IGNORE])
