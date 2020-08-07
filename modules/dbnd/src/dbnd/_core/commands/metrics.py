import logging
import time
import typing

from functools import wraps

from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd._core.task_run.task_run_tracker import (
    TaskRunTracker,
    get_value_meta_for_metric,
)
from dbnd._core.tracking.histograms import HistogramRequest
from dbnd._core.utils import seven
from targets.value_meta import ValueMeta, ValueMetaConf


if typing.TYPE_CHECKING:
    from typing import Any, Optional, Union, List
    import pandas as pd
    import pyspark.sql as spark

    from dbnd_postgres.postgres_values import PostgresTable

logger = logging.getLogger(__name__)


def _get_tracker():
    # type: ()-> Optional[TaskRunTracker]
    from dbnd._core.task_run.current_task_run import try_get_or_create_task_run

    task_run = try_get_or_create_task_run()
    if task_run:
        return task_run.tracker
    return None


def log_data(
    key,  # type: str
    value,  # type: Union[pd.DataFrame, spark.DataFrame, PostgresTable]
    path=None,  # type: Optional[str]
    access_type=DbndTargetOperationType.read,  # type: DbndTargetOperationType
    with_preview=True,  # type: Optional[bool]
    with_size=True,  # type: Optional[bool]
    with_schema=True,  # type: Optional[bool]
    with_stats=False,  # type: Optional[bool]
    with_histograms=HistogramRequest.NONE(),  # type: Optional[Union[bool, str, List[str], HistogramRequest]]
):  # type: (...) -> None

    meta_conf = ValueMetaConf(
        log_preview=with_preview,
        log_schema=with_schema,
        log_size=with_size,
        log_stats=with_stats,
        log_df_hist=with_histograms,
    )
    tracker = _get_tracker()
    histogram_request = HistogramRequest.from_with_histograms(with_histograms)

    if path:
        log_target(value, path, access_type, meta_conf, histogram_request)

    if tracker:
        tracker.log_data(
            key, value, meta_conf=meta_conf, histogram_request=histogram_request
        )
        return

    from dbnd._core.task_run.task_run_tracker import get_value_meta_for_metric

    value_type = get_value_meta_for_metric(key, value, meta_conf, histogram_request)
    if value_type:
        logger.info("Log data '{}': shape='{}'".format(key, value_type.data_dimensions))
    else:
        logger.info("Log data '{}': {} is not supported".format(key, type(value)))


def log_target(
    value,  # type: Any
    path,  # type: str
    access_type=DbndTargetOperationType.write,  # type: DbndTargetOperationType
    meta_conf=None,  # type: Optional[ValueMetaConf]
    histogram_request=HistogramRequest.NONE(),  # type: HistogramRequest
):  # type: (...) -> None
    tracker = _get_tracker()
    if tracker:
        if meta_conf:
            value_meta = get_value_meta_for_metric(
                path, value, meta_conf, histogram_request
            )
        else:
            value_meta = ValueMeta(value_preview="<N/A>")
        tracker.tracking_store.log_target(
            task_run=tracker.task_run,
            target=path,
            target_meta=value_meta,
            operation_type=access_type,
            operation_status=DbndTargetOperationStatus.OK,
        )


log_dataframe = log_data


def log_pg_table(
    table_name,
    connection_string,
    with_preview=True,
    with_schema=True,
    with_histograms=HistogramRequest.NONE(),  # type: Union[HistogramRequest, bool, str, List[str]]
):

    try:
        if not is_plugin_enabled("dbnd-postgres", module_import="dbnd_postgres"):
            return
        from dbnd_postgres import postgres_values

        pg_table = postgres_values.PostgresTable(table_name, connection_string)
        log_data(
            table_name,
            pg_table,
            with_preview=with_preview,
            with_schema=with_schema,
            with_histograms=with_histograms,
        )
    except Exception:
        logger.exception("Failed to log_pg_table")


def log_metric(key, value, source="user"):
    tracker = _get_tracker()
    if tracker:
        tracker.log_metric(key, value, source=source)
        return

    logger.info("Log {} Metric '{}'='{}'".format(source.capitalize(), key, value))


def log_artifact(key, artifact):
    tracker = _get_tracker()
    if tracker:
        tracker.log_artifact(key, artifact)
        return

    logger.info("Artifact %s=%s", key, artifact)


def log_function_duration(metric_key):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = f(*args, **kwargs)
            end_time = time.time()
            log_metric(metric_key, end_time - start_time)
            return result

        return wrapper

    return decorator


@seven.contextlib.contextmanager
def log_duration(metric_key):
    start_time = time.time()
    yield
    end_time = time.time()
    log_metric(metric_key, end_time - start_time)
