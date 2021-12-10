import logging
import os
import typing

from typing import Optional

from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import try_get_current_task_run


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


def try_get_or_create_task_run():
    # type: ()-> Optional[TaskRun]
    task_run = try_get_current_task_run()
    if task_run:
        return task_run

    from dbnd._core.configuration.environ_config import DBND_TASK_RUN_ATTEMPT_UID

    tra_uid = os.environ.get(DBND_TASK_RUN_ATTEMPT_UID)
    if tra_uid:
        return _get_task_run_mock(tra_uid)

    from dbnd._core.tracking.script_tracking_manager import (
        try_get_inplace_tracking_task_run,
    )

    return try_get_inplace_tracking_task_run()


def _get_task_run_mock(tra_uid):
    """
    We need better implementation for this,
    currently in use only for spark
    """
    try:
        from dbnd._core.task_run.task_run_tracker import TaskRunTracker

        task_run = TaskRunMock(tra_uid)
        from dbnd import config
        from dbnd._core.settings import CoreConfig

        with config(
            {CoreConfig.tracker_raise_on_error: False}, source="on_demand_tracking"
        ):
            with new_dbnd_context(
                name="fast_dbnd_context", autoload_modules=False
            ) as fast_dbnd_ctx:
                trt = TaskRunTracker(task_run, fast_dbnd_ctx.tracking_store)
                task_run.tracker = trt
                return task_run
    except Exception:
        logger.info("Failed during dbnd inplace tracking init.", exc_info=True)
        return None


class TaskRunMock(object):
    def __init__(self, task_run_attempt_uid):
        self.task_run_attempt_uid = task_run_attempt_uid

    def __getattr__(self, item):
        return self.__dict__.get(item, self)
