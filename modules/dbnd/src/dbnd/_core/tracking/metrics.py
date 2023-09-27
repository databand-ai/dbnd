# © Copyright Databand.ai, an IBM Company 2022

import logging
import time
import typing

from typing import Any, Dict, List, Optional, Union

from dbnd._core.configuration.environ_config import is_dbnd_enabled
from dbnd._core.constants import DbndDatasetOperationType, DbndTargetOperationType
from dbnd._core.task_run.task_run_tracker import DatasetOperationReport, TaskRunTracker
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.utils import seven
from dbnd._core.utils.dbnd_modules import is_module_enabled
from dbnd._core.utils.one_time_logger import get_one_time_logger
from targets import Target
from targets.value_meta import ValueMetaConf


LOG_DATASET_OP_OP_SOURCE = "python_manual_logging"

if typing.TYPE_CHECKING:
    from datetime import datetime

    import pandas as pd
    import pyspark.sql as spark

    from dbnd_postgres.postgres_values import PostgresTable

logger = logging.getLogger(__name__)

TRACKER_MISSING_MESSAGE = "Can't report %s because no tracker was found (did you use `with dbnd_tracking()` or `export DBND__TRACKING=True`?)"


def _get_tracker():
    # type: ()-> Optional[TaskRunTracker]
    """
    Look for a tracker of running task_run_executor or initiate a task_run_executor if nothing is running.
    Will return a None if there is no exist task_run_executor nor couldn't start one.
    """
    if not is_dbnd_enabled():
        return None
    from dbnd._core.task_run.current_task_run import try_get_or_create_task_run

    task_run = try_get_or_create_task_run()
    if task_run:
        return task_run.tracker
    return None


def log_data(
    key,  # type: str
    value=None,  # type: Union[pd.DataFrame, spark.DataFrame, PostgresTable]
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
    if not is_dbnd_enabled():
        return

    tracker = _get_tracker()
    if not tracker:
        message = TRACKER_MISSING_MESSAGE % ("log_data",)
        get_one_time_logger().log_once(message, "log_data", logging.WARNING)
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
log_dataframe.__doc__ = """
    Logs a dataframe to dbnd.

    Args:
        key: Name of the dataframe.
        value: The dataframe itself.
        path: Optional target or path representing a target to connect the dataframe to.
        operation_type: Type of the operation doing with the target - reading or writing the dataframe?
        with_preview: True if should log a preview of the dataframe.
        with_size: True if should log the size of the dataframe.
        with_schema: True if should log the schema of the dataframe.
        with_stats: True if should calculate and log stats of the dataframe.
        with_histograms: True if should calculate and log histogram of the dataframe.
        raise_on_error: raise if error occur.

    Example::

        @task
        def process_customers_data(data) -> pd.DataFrame:
            log_dataframe("customers_data", data)
"""


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
    if not is_dbnd_enabled():
        return

    try:
        if not is_module_enabled("dbnd_postgres"):
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

    Args:
        key: Name of the metric.
        value: Value of the metric.
        source: The source of the metric, default is user.

    Example::

        def calculate_alpha(alpha):
            alpha *= 1.1
            log_metric("alpha", alpha)
    """
    if not is_dbnd_enabled():
        return

    tracker = _get_tracker()
    if tracker:
        tracker.log_metric(key, value, source=source)
        return

    message = TRACKER_MISSING_MESSAGE % ("log_metric",)
    get_one_time_logger().log_once(message, "log_metric", logging.WARNING)
    logger.info("Log {} Metric '{}'='{}'".format(source.capitalize(), key, value))
    return


def log_metrics(metrics_dict, source="user", timestamp=None):
    # type: (Dict[str, Any], str, datetime) -> None
    """
    Log multiple key-value pairs as metrics to dbnd.

    Args:
        metrics_dict: Key-value pairs of metrics to log.
        source: Optional name of the metrics source, default is user.
        timestamp: Optional timestamp of the metrics.

    Example::

        @task
        def log_lowercase_letters():
            # all lower alphabet chars -> {"a": 97,..., "z": 122}
            log_metrics({chr(i): i for i in range(97, 123)})
    """
    if not is_dbnd_enabled():
        return

    tracker = _get_tracker()
    if tracker:
        tracker.log_metrics(metrics_dict, source=source, timestamp=timestamp)
        return

    message = TRACKER_MISSING_MESSAGE % ("log_metrics",)
    get_one_time_logger().log_once(message, "log_metrics", logging.WARNING)
    logger.info(
        "Log multiple metrics from {source}: {metrics}".format(
            source=source.capitalize(), metrics=metrics_dict
        )
    )


def log_artifact(key, artifact):
    """
    Log a local file or directory as an artifact of the currently active run.

    Args:
        key: The key by which to log the artifact
        artifact: The artifact to log

    Example::

        def prepare_data(data):
            lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt"
            data.write(lorem)
            log_artifact("my_tmp_file", str(data))
    """
    if not is_dbnd_enabled():
        return

    tracker = _get_tracker()
    if tracker:
        tracker.log_artifact(key, artifact)
        return

    message = TRACKER_MISSING_MESSAGE % ("log_artifact",)
    get_one_time_logger().log_once(message, "log_artifact", logging.WARNING)
    logger.info("Artifact %s=%s", key, artifact)


@seven.contextlib.contextmanager
def log_duration(metric_key, source="user"):
    """
    Measure time of function or code block, and log to Databand as a metric.

    Can be used as a decorator and in "with" statement as a context manager.

    Example 1::

        @log_duration("f_time_duration")
        def f():
            sleep(1)

    Example 2::

        with log_duration("my_code_duration"):
            sleep(1)
    """
    if not is_dbnd_enabled():
        yield
        return

    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        log_metric(metric_key, end_time - start_time, source)


def _report_operation(operation_report):
    # type: (DatasetOperationReport) -> None
    tracker = _get_tracker()
    if not tracker:
        message = TRACKER_MISSING_MESSAGE % ("report_operation",)
        get_one_time_logger().log_once(message, "report_operation", logging.WARNING)
        return

    tracker.log_dataset(op_report=operation_report)


def log_dataset_op(
    op_path: Union[Target, str],
    op_type: Union[DbndDatasetOperationType, str],
    *,
    success: bool = True,
    error: Optional[str] = None,
    data: Optional[Any] = None,
    with_histograms: Optional[Union[bool, str, List[str], LogDataRequest]] = None,
    with_partition: Optional[bool] = None,
    with_stats: Optional[Union[bool, str, List[str], LogDataRequest]] = True,
    with_preview: bool = False,
    with_schema: bool = True,
    send_metrics: bool = True,
    row_count: Optional[int] = None,
    column_count: Optional[int] = None,
    operation_source: str = LOG_DATASET_OP_OP_SOURCE,
):
    """
    Logs dataset operation and meta data to dbnd.

    Args:
        op_path: Target object to log or a unique path representing the operation location.
        op_type: Type of operation that been done with the target - read, write, delete.
        success: True if the operation succeeded, False otherwise.
        error: Optional error message.
        data: Optional value of data to use build meta-data on the target.
        with_histograms: Should calculate histogram of the given data - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_stats: Should extract schema of the data as meta-data of the target - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_partition: If True, the webserver tries to detect partitions of our datasets and extract them from the path,
                        otherwise not manipulating the dataset path at all.
        with_preview: Should extract preview of the data as meta-data of the target - relevant only with data param.
        with_schema: Should extract schema of the data as meta-data of the target - relevant only with data param.
        send_metrics: Should report preview, schemas and histograms as metrics.
        row_count: Should report row count no matter what is the data
        column_count: Should report column count no matter what is the data
        operation_source: Optional source of operation, such as: redshift_tracker, dbt_sdk, etc...
    Example::

        @task
        def prepare_data():
            log_dataset_op(
                "/path/to/value.csv",
                DbndDatasetOperationType.read,
                data=pandas_data_frame,
                with_preview=True,
                with_schema=True,
            )
    """
    if not is_dbnd_enabled():
        return

    operation_report = DatasetOperationReport(
        op_path=op_path,
        op_type=op_type,
        data=data,
        success=success,
        error=error,
        with_preview=with_preview,
        with_schema=with_schema,
        with_histograms=with_histograms,
        with_stats=with_stats,
        send_metrics=send_metrics,
        with_partition=with_partition,
        row_count=row_count,
        column_count=column_count,
        operation_source=operation_source,
    )
    _report_operation(operation_report)


@seven.contextlib.contextmanager
def dataset_op_logger(
    op_path: Union[Target, str],
    op_type: Union[DbndDatasetOperationType, str],
    *,
    data: Optional[Any] = None,
    with_histograms: Optional[bool] = None,
    with_partition: Optional[bool] = None,
    with_stats: Optional[bool] = True,
    with_preview: bool = False,
    with_schema: bool = True,
    send_metrics: bool = True,
    row_count: Optional[int] = None,
    column_count: Optional[int] = None,
    operation_source: str = LOG_DATASET_OP_OP_SOURCE,
):
    """
    Wrapper to Log dataset operation and meta data to dbnd.

    Make sure to only wrap operation related code!

    Args:
        op_path: Target object to log or a unique path representing the target logic location.
        op_type: Type of operation that been done with the dataset - read, write, delete.
        data: Optional value of data to use build meta-data on the dataset.
        with_histograms: Should calculate histogram of the given data - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_stats: Should extract schema of the data as meta-data of the target - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_partition: If True, the webserver tries to detect partitions of our datasets and extract them from the path,
                        otherwise not manipulating the dataset path at all.
        with_preview: Should extract preview of the data as meta-data of the dataset - relevant only with data param.
        with_schema: Should extract schema of the data as meta-data of the dataset - relevant only with data param.
        send_metrics: Should report preview, schemas and histograms as metrics.
        row_count: should report row count no matter what is the data
        column_count: should report column count no matter what is the data
        operation_source: Optional source of operation, such as: redshift tracker, dbt sdk, etc...
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
            # If unrelated_func raises an exception, failed read operation is reported to databand.
    """
    operation_report = DatasetOperationReport(
        op_path=op_path,
        op_type=op_type,
        data=data,
        with_preview=with_preview,
        with_schema=with_schema,
        with_histograms=with_histograms,
        with_stats=with_stats,
        send_metrics=send_metrics,
        with_partition=with_partition,
        row_count=row_count,
        column_count=column_count,
        operation_source=operation_source,
    )

    try:
        yield operation_report
    except Exception as e:
        operation_report.set_error(error=str(e))
        raise
    else:
        operation_report.set_success()
    finally:
        _report_operation(operation_report)
