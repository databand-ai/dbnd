import atexit
import logging
import os
import sys
import typing

from subprocess import list2cmdline
from typing import Optional

from dbnd._core.configuration import environ_config
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import RunState, TaskRunState, UpdateSource
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import is_verbose, try_get_databand_run
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
    AirflowOperatorRuntimeTask,
    override_airflow_log_system_for_tracking,
    try_get_airflow_context,
)
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun

    T = typing.TypeVar("T")


def is_inplace_run():
    if environ_config.environ_enabled(environ_config.ENV_DBND__TRACKING):
        return True
    airflow_context = try_get_airflow_context()
    if airflow_context:
        return True
    return False


def set_tracking_config_overide(airflow_context=None, use_dbnd_log=None):
    # 1. create proper DatabandContext so we can create other objects
    track_with_cache = config.getboolean("run", "tracking_with_cache")
    config_for_tracking = {
        "run": {
            "skip_completed": track_with_cache,
            "skip_completed_on_run": track_with_cache,
            "validate_task_inputs": track_with_cache,
            "validate_task_outputs": track_with_cache,
        },  # we don't want to "check" as script is task_version="now"
        "task": {
            "task_in_memory_outputs": not track_with_cache
        },  # do not save any outputs
        "core": {"tracker_raise_on_error": False},  # do not fail on tracker errors
    }
    if airflow_context:
        import pytz

        task_target_date = pendulum.parse(
            airflow_context.execution_date, tz=pytz.UTC
        ).date()
        use_dbnd_log = override_airflow_log_system_for_tracking()
        config_for_tracking["task"]["task_target_date"] = task_target_date

    if use_dbnd_log is not None:
        config_for_tracking["log"] = {"disabled": not use_dbnd_log}
    return config.set_values(
        config_values=config_for_tracking, override=True, source="dbnd_tracking_config"
    )


class _DbndInplaceRunManager(object):
    def __init__(self):
        self._context_managers = []
        self._atexit_registered = False

        self._started = False
        self._stoped = False
        self._disabled = False

        self._run = None
        self._task_run = None

    def _enter_cm(self, cm):
        # type: (typing.ContextManager[T]) -> T
        # else contextManagers are getting closed sometimes :(
        val = cm.__enter__()
        self._context_managers.append(cm)
        return val

    def _close_all_context_managers(self):
        while self._context_managers:
            cm = self._context_managers.pop()
            cm.__exit__(None, None, None)

    def start(self, root_task_name, job_name=None):
        if self._run:
            return
        if self._started or self._disabled:  # started or failed
            return

        try:
            if try_get_databand_run():
                return

            self.tracking_context(root_task_name=root_task_name, job_name=job_name)
            self._started = True
        except Exception:
            _handle_inline_error("inline-start")
            self._disabled = True
            return
        finally:
            self._started = True

    def tracking_context(self, root_task_name, job_name=None):
        airflow_context = try_get_airflow_context()
        set_tracking_config_overide(use_dbnd_log=True, airflow_context=airflow_context)

        # 1. create proper DatabandContext so we can create other objects
        dc = self._enter_cm(new_dbnd_context())  # type: DatabandContext

        if airflow_context:
            root_task_or_task_name = AirflowOperatorRuntimeTask.build_from_airflow_context(
                airflow_context
            )
            source = UpdateSource.airflow_tracking
            job_name = "{}.{}".format(airflow_context.dag_id, airflow_context.task_id)
        else:
            root_task_or_task_name = _build_inline_root_task(root_task_name)
            source = UpdateSource.dbnd

        # create databand run
        # this will create databand run with driver and root tasks.

        # create databand run
        # we will want to preserve
        self._run = self._enter_cm(
            new_databand_run(
                context=dc,
                task_or_task_name=root_task_or_task_name,
                job_name=job_name,
                existing_run=False,
                source=source,
                af_context=airflow_context,
            )
        )  # type: DatabandRun

        self._run._init_without_run()

        if not self._atexit_registered:
            atexit.register(self.stop)
        sys.excepthook = self.stop_on_exception

        self._start_taskrun(self._run.driver_task_run)
        self._start_taskrun(self._run.root_task_run)
        self._task_run = self._run.root_task_run
        return self._task_run

    def _start_taskrun(self, task_run):
        self._enter_cm(task_run.runner.task_run_execution_context())
        task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def stop(self):
        if self._stoped:
            return
        try:
            databand_run = self._run
            root_tr = self._task_run
            root_tr.finished_time = utcnow()

            if root_tr.task_run_state not in TaskRunState.finished_states():
                for tr in databand_run.task_runs:
                    if tr.task_run_state == TaskRunState.FAILED:
                        root_tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED)
                        databand_run.set_run_state(RunState.FAILED)
                        break
                else:
                    root_tr.set_task_run_state(TaskRunState.SUCCESS)

            if root_tr.task_run_state == TaskRunState.SUCCESS:
                databand_run.set_run_state(RunState.SUCCESS)
            else:
                databand_run.set_run_state(RunState.FAILED)
            logger.info(databand_run.describe.run_banner_for_finished())

            self._close_all_context_managers()
        except Exception as ex:
            _handle_inline_error("dbnd-tracking-shutdown")
        finally:
            self._stoped = True

    def stop_on_exception(self, type, value, traceback):
        if not self._stoped:
            try:
                error = TaskRunError.buid_from_ex(
                    ex=value, task_run=self._task_run, exc_info=(type, value, traceback)
                )
                self._task_run.set_task_run_state(TaskRunState.FAILED, error=error)
            except:
                _handle_inline_error("dbnd-set-script-error")

        self.stop()
        sys.__excepthook__(type, value, traceback)


def _build_inline_root_task(root_task_name):
    # create "root task" with default name as current process executable file name
    if not root_task_name:
        root_task_name = sys.argv[0].split(os.path.sep)[-1]

    class InplaceTask(Task):
        _conf__task_family = root_task_name

    try:
        user_frame = UserCodeDetector.build_code_detector().find_user_side_frame(
            user_side_only=True
        )
        if user_frame:
            module_code = open(user_frame.filename).read()
            InplaceTask.task_definition.task_module_code = module_code
            InplaceTask.task_definition.task_source_code = module_code
    except Exception as ex:
        logger.info("Failed to find source code: %s", str(ex))

    root_task = InplaceTask(task_version="now", task_name=root_task_name)

    root_task.task_meta.task_command_line = list2cmdline(sys.argv)
    root_task.task_meta.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)

    return root_task


# there can be only one tracking manager
_dbnd_start_manager = None  # type: Optional[_DbndInplaceRunManager]


def get_dbnd_inplace_run_manager():
    global _dbnd_start_manager
    if _dbnd_start_manager is False:
        # we already failed to create ourself once
        return None

    try:
        # this part will run DAG and Operator Tasks
        if _dbnd_start_manager is None:
            # this is our first call!

            _dbnd_start_manager = _DbndInplaceRunManager()
        return _dbnd_start_manager
    except Exception:
        _dbnd_start_manager = False
        logger.warning("Error during dbnd pre-init, ignoring", exc_info=True)
        return None


def dbnd_run_start(name=None):
    dsm = get_dbnd_inplace_run_manager()
    if not dsm:
        return
    return dsm.start(root_task_name=name)


def dbnd_run_stop():
    dsm = get_dbnd_inplace_run_manager()
    if not dsm:
        return
    _dbnd_start_manager.stop()


def _handle_inline_error(msg, func_call=None):
    if is_verbose():
        logger.warning(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg,
            exc_info=True,
        )
    else:
        logger.info(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg
        )

    global _dbnd_start_manager
    _dbnd_start_manager = False
    return func_call.invoke()
