import logging
import time
import typing

import attr

from dbnd._core.constants import (
    DbndDatasetOperationType,
    DbndTargetOperationStatus,
    DbndTargetOperationType,
)
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.utils import seven
from targets import Target
from targets.value_meta import ValueMeta, ValueMetaConf


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
    """
    Look for a tracker of running task_run or initiate a task_run if nothing is running.
    Will return a None if there is no exist task_run nor couldn't start one.
    """
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
    """
    Log data information to dbnd.

    @param key: Name of the data.
    @param value: Value of the data, currently supporting only dataframes and tables view.
    @param path: Optional target or path representing a target to connect the data to.
    @param operation_type: Type of the operation doing with the target - reading or writing the data?
    @param with_preview: True if should log a preview of the data.
    @param with_size: True if should log the size of the data.
    @param with_schema: True if should log the schema of the data.
    @param with_stats: True if should calculate and log stats of the data.
    @param with_histograms: True if should calculate and log histogram of the data.
    @param raise_on_error: raise if error occur.
    """

    tracker = _get_tracker()
    if not tracker:
        logger.warning(
            "Couldn't log data - {key}. Tracker is not found".format(key=key)
        )
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


# logging dataframe is the same as logging data
log_dataframe = log_data


def log_pg_table(
    table_name,
    connection_string,
    with_preview=None,  # type: Optional[bool]
    with_schema=None,  # type: Optional[bool]
    with_histograms=None,  # type: Union[LogDataRequest, bool, str, List[str]]
):
    """
    Log the data of postgres table to dbnd.

    @param table_name: name of the table to log
    @param connection_string: a connection string used to reach the table.
    @param with_preview: True if should log a preview of the table.
    @param with_schema: True if should log the schema of the table.
    @param with_histograms: True if should calculate and log histogram of the table data.
    """
    try:
        if not is_plugin_enabled("dbnd-postgres", module_import="dbnd_postgres"):
            logger.warning(
                "Can't log postgres table: dbnd-postgres package is not installed\n"
                "Help: pip install dbnd-postgres"
            )
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
    # type: (str, Any, Optional[str]) -> None
    """
    Log key-value pair as a metric to dbnd.

    @param key: Name of the metric.
    @param value: Value of the metric.
    @param source: The source of the metric, default is user.
    """
    tracker = _get_tracker()
    if tracker:
        tracker.log_metric(key, value, source=source)
        return

    logger.info("Log {} Metric '{}'='{}'".format(source.capitalize(), key, value))


def log_metrics(metrics_dict, source="user", timestamp=None):
    # type: (Dict[str, Any], str, datetime) -> None
    """
    Log multiple key-value pairs as a metrics to dbnd.

    @param metrics_dict: Key-value pairs of metrics to log.
    @param source: Optional name of the metrics source, default is user.
    @param timestamp: Optional timestamp of the metrics.
    """

    tracker = _get_tracker()
    if tracker:
        tracker.log_metrics(metrics_dict, source=source, timestamp=timestamp)
        return

    logger.info(
        "Log multiple metrics from {source}: {metrics}".format(
            source=source.capitalize(), metrics=metrics_dict
        )
    )


def log_artifact(key, artifact):
    tracker = _get_tracker()
    if tracker:
        tracker.log_artifact(key, artifact)
        return

    logger.info("Artifact %s=%s", key, artifact)


def log_dataset_op(
    op_path,  # type: Union[Target,str]
    op_type,  # type: Union[DbndDatasetOperationType, str]
    success=True,  # type: bool
    data=None,
    with_preview=None,
    with_schema=None,
    with_histograms=None,
    send_metrics=True,
):
    """
    Logs dataset operation and meta data to dbnd.

    @param op_path: Target object to log or a unique path representing the operation location
    @param op_type: the type of operation that been done with the target - read, write, delete.
    @param success: True if the operation succeeded, False otherwise.
    @param data: optional value of data to use build meta-data on the target
    @param with_preview: should extract preview of the data as meta-data of the target - relevant only with data param
    @param with_schema: should extract schema of the data as meta-data of the target - relevant only with data param
    @param with_histograms:
    """
    if isinstance(op_type, str):
        # If str type is not of DbndDatasetOperationType, raise.
        op_type = DbndDatasetOperationType[op_type]

    tracker = _get_tracker()
    if tracker:
        meta_conf = ValueMetaConf(
            log_preview=with_preview,
            log_schema=with_schema,
            log_size=with_schema,
            log_stats=with_histograms,
            log_histograms=with_histograms,
        )

        status = (
            DbndTargetOperationStatus.OK if success else DbndTargetOperationStatus.NOK
        )

        tracker.log_dataset(
            operation_path=op_path,
            operation_type=op_type,
            operation_status=status,
            data=data,
            meta_conf=meta_conf,
            send_metrics=send_metrics,
        )
        return

    logger.info(
        "Operation {operation} executed {status} on target {path}.".format(
            operation=op_type,
            status=("successfully" if success else "unsuccessfully"),
            path=str(op_path),
        )
    )


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


@attr.s(slots=True, frozen=False)
class DatasetOperationLogger(object):
    op_path = attr.ib(default=None)
    op_type = attr.ib(default=None)
    data = attr.ib(default=None)
    with_preview = attr.ib(default=None)
    with_schema = attr.ib(default=None)
    with_histograms = attr.ib(default=None)
    send_metrics = attr.ib(default=None)

    def set(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                logger.warning(
                    "Can't set attribute {} for DatasetOperationLogger, no such attribute".format(
                        k
                    )
                )


@seven.contextlib.contextmanager
def dataset_op_logger(
    op_path,  # type: Union[Target,str]
    op_type,  # type: Union[DbndDatasetOperationType, str]
    data=None,
    with_preview=True,
    with_schema=True,
    with_histograms=False,
    send_metrics=True,
):
    """
    Wrapper to Log dataset operation and meta data to dbnd.
    ** Make sure to wrap only operation related code **

    Good Example::

        with dataset_op_logger("location://path/to/value.csv", "read"):
            value = read_from()
            # Read is successful

        unrelated_func()

    Bad Example::

        with dataset_op_logger("location://path/to/value.csv", "read"):
            value = read_from()
            # Read is successful
            unrelated_func()
            # If unrelated_func raise exception, failed read operation is reported to databand.

    @param op_path: Target object to log or a unique path representing the target logic location
    @param op_type: the type of operation that been done with the dataset - read, write, delete.
    @param data: optional value of data to use build meta-data on the dataset
    @param with_preview: should extract preview of the data as meta-data of the dataset - relevant only with data param
    @param with_schema: should extract schema of the data as meta-data of the dataset - relevant only with data param
    @param with_histograms: should calculate histogram and stats of the given data - relevant only with data param
    @param send_metrics: should report preview, schemas and histograms as metrics
    """
    if isinstance(op_type, str):
        # If str type is not of DbndDatasetOperationType, raise.
        op_type = DbndDatasetOperationType[op_type]

    op = DatasetOperationLogger(
        op_path, op_type, data, with_preview, with_schema, with_histograms, send_metrics
    )

    try:
        yield op
    except Exception:
        log_dataset_op(success=False, **attr.asdict(op))
        raise
    else:
        log_dataset_op(success=True, **attr.asdict(op))
