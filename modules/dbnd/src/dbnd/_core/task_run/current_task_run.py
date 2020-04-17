import os

from dbnd import dbnd_run_start
from dbnd._core.current import try_get_current_task_run
from dbnd._vendor.colorlog import logging


logger = logging.getLogger(__name__)


def try_get_or_create_task_run():
    # type: ()-> TaskRunTracker
    task_run = try_get_current_task_run()
    if task_run:
        return task_run

    try:
        from dbnd._core.task_run.task_run_tracker import TaskRunTracker
        from dbnd._core.configuration.environ_config import DBND_TASK_RUN_ATTEMPT_UID

        tra_uid = os.environ.get(DBND_TASK_RUN_ATTEMPT_UID)
        if tra_uid:
            task_run = TaskRunMock(tra_uid)
            from dbnd import config
            from dbnd._core.settings import CoreConfig

            with config(
                {CoreConfig.tracker_raise_on_error: False}, source="ondemand_tracking"
            ):
                tracking_store = CoreConfig().get_tracking_store()
                trt = TaskRunTracker(task_run, tracking_store)
                task_run.tracker = trt
                return task_run

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
                return atm.airflow_operator__task_run
        from dbnd._core.inplace_run.inplace_run_manager import is_inplace_run

        if is_inplace_run():
            return dbnd_run_start()

    except Exception:
        logger.info("Failed during dbnd inplace tracking init.", exc_info=True)
        return None


# We need better implementation for this,
# currently in use only for spark


class TaskRunMock(object):
    def __init__(self, task_run_attempt_uid):
        self.task_run_attempt_uid = task_run_attempt_uid

    def __getattr__(self, item):
        return self.__dict__.get(item, self)
