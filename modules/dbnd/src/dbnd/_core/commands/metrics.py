import logging
import os
import typing


if typing.TYPE_CHECKING:
    from typing import Optional, Union
    import pandas as pd
    import pyspark.sql as spark


logger = logging.getLogger(__name__)


def log_dataframe(key, value, with_preview=True):
    # type: (str, Union[pd.DataFrame, spark.DataFrame], Optional[bool]) -> None

    from dbnd._core.task_build.task_context import current, has_current_task

    tracker = current() if has_current_task() else _get_ondemand_tracker()
    if not tracker:
        from dbnd._core.task_run.task_run_tracker import get_value_meta_for_metric

        value_type = get_value_meta_for_metric(key, value, with_preview=with_preview)
        if value_type:
            logger.info(
                "Log DataFrame '{}': shape='{}'".format(key, value_type.data_dimensions)
            )
        return

    return tracker.log_dataframe(key, value, with_preview)


def log_metric(key, value, source="user"):
    from dbnd._core.task_build.task_context import current, has_current_task

    tracker = current() if has_current_task() else _get_ondemand_tracker()
    if not tracker:
        logger.info("Log {} Metric '{}'='{}'".format(source.capitalize(), key, value))
        return

    return tracker.log_metric(key, value, source=source)


def log_artifact(key, artifact):
    from dbnd._core.task_build.task_context import current, has_current_task

    tracker = current() if has_current_task() else _get_ondemand_tracker()
    if not tracker:
        logger.info("Artifact %s=%s", key, artifact)
        return

    return tracker.log_artifact(key, artifact)


def _get_ondemand_tracker():
    try:
        from dbnd._core.task_run.task_run_tracker import TaskRunTracker
        from dbnd._core.configuration.environ_config import DBND_TASK_RUN_ATTEMPT_UID

        tra_uid = os.environ.get(DBND_TASK_RUN_ATTEMPT_UID)
        if tra_uid:
            task_run = TaskRunMock(tra_uid)
            trt = TaskRunTracker(task_run, _get_ondemand_tracking_store())
            return trt

        # let's check if we are in airflow env
        from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
            try_get_airflow_context,
        )

        airflow_context = try_get_airflow_context()
        if airflow_context:
            from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
                get_airflow_tracking_manager,
            )

            atm = get_airflow_tracking_manager(airflow_context)
            if atm:
                return atm.airflow_operator__task_run.tracker

    except Exception:
        logger.warning(
            "Failed during dbnd context-metrics-enter, ignoring", exc_info=True
        )
        return None


def _get_ondemand_tracking_store():
    from dbnd import config
    from dbnd._core.settings import CoreConfig

    with config({CoreConfig.tracker_raise_on_error: False}, source="ondemand_tracking"):
        tracking_store = CoreConfig().get_tracking_store()
        return tracking_store


class TaskRunMock(object):
    def __init__(self, task_run_attempt_uid):
        self.task_run_attempt_uid = task_run_attempt_uid

    def __getattr__(self, item):
        return self.__dict__.get(item, self)
