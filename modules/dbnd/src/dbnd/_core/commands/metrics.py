import logging
import time
import typing

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.utils import seven
from targets.value_meta import ValueMetaConf


if typing.TYPE_CHECKING:
    from datetime import datetime
    from typing import Optional, Union, List, Dict, Any
    import pandas as pd
    import pyspark.sql as spark

    from dbnd_postgres.postgres_values import PostgresTable
    from dbnd_snowflake.snowflake_values import SnowflakeTable

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
    value=None,  # type: Union[pd.DataFrame, spark.DataFrame, PostgresTable, SnowflakeTable]
    path=None,  # type: Optional[str]
    operation_type=DbndTargetOperationType.read,  # type: DbndTargetOperationType
    with_preview=None,  # type: Optional[bool]
    with_size=None,  # type: Optional[bool]
    with_schema=None,  # type: Optional[bool]
    with_stats=None,  # type: Optional[Union[bool, str, List[str], LogDataRequest]]
    with_histograms=None,  # type: Optional[Union[bool, str, List[str], LogDataRequest]]
    raise_on_error=False,  # type: bool
):  # type: (...) -> None
    tracker = _get_tracker()
    if not tracker:
        return

    meta_conf = ValueMetaConf(
        log_preview=with_preview,
        log_schema=with_schema,
        log_size=with_size,
        log_stats=with_stats,
        log_histograms=with_histograms,
    )

    tracker.log_data(
        key,
        value,
        meta_conf=meta_conf,
        path=path,
        operation_type=operation_type,
        raise_on_error=raise_on_error,
    )


log_dataframe = log_data


def log_pg_table(
    table_name,
    connection_string,
    with_preview=None,  # type: Optional[bool]
    with_schema=None,  # type: Optional[bool]
    with_histograms=None,  # type: Union[LogDataRequest, bool, str, List[str]]
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


def log_metrics(metrics_dict, source="user", timestamp=None):
    # type: (Dict[str, Any], str, datetime) -> None
    tracker = _get_tracker()
    if tracker:
        from dbnd._core.tracking.schemas.metrics import Metric
        from dbnd._core.utils.timezone import utcnow

        metrics = [
            Metric(key=key, value=value, source=source, timestamp=timestamp or utcnow())
            for key, value in metrics_dict.items()
        ]
        tracker._log_metrics(metrics)


def log_artifact(key, artifact):
    tracker = _get_tracker()
    if tracker:
        tracker.log_artifact(key, artifact)
        return

    logger.info("Artifact %s=%s", key, artifact)


@seven.contextlib.contextmanager
def log_duration(metric_key, source="user"):
    """
    Measure time of function or code block, and log to Databand as a metric.
    Can be used as a decorator and in "with" statement as a context manager.

    Example 1:
        @log_duration("f_time_duration")
        def f():
            sleep(1)

    Example 2:
        with log_duration("my_code_duration"):
            sleep(1)
    """
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        log_metric(metric_key, end_time - start_time, source)
