import logging
import typing

from dbnd._core.task_run.task_run_tracker import TaskRunTracker


if typing.TYPE_CHECKING:
    from typing import Optional, Union
    import pandas as pd
    import pyspark.sql as spark


logger = logging.getLogger(__name__)


def _get_tracker():
    # type: ()-> Optional[TaskRunTracker]
    from dbnd._core.task_run.current_task_run import try_get_or_create_task_run

    task_run = try_get_or_create_task_run()
    if task_run:
        return task_run.tracker
    return None


def log_dataframe(key, value, with_preview=True):
    # type: (str, Union[pd.DataFrame, spark.DataFrame], Optional[bool]) -> None

    tracker = _get_tracker()
    if tracker:
        tracker.log_dataframe(key, value, with_preview)
        return

    from dbnd._core.task_run.task_run_tracker import get_value_meta_for_metric

    value_type = get_value_meta_for_metric(key, value, with_preview=with_preview)
    if value_type:
        logger.info(
            "Log DataFrame '{}': shape='{}'".format(key, value_type.data_dimensions)
        )
    else:
        logger.info("Log DataFrame '{}': {} is not supported".format(key, type(value)))


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
