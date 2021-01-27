import atexit
import logging
import os
import sys
import typing

from subprocess import list2cmdline
from typing import Optional

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import RunState, TaskRunState, UpdateSource
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import is_verbose, try_get_databand_run
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.settings import CoreConfig
from dbnd._core.task.task import Task
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    build_run_time_airflow_task,
    override_airflow_log_system_for_tracking,
)
from dbnd._core.utils import seven
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun

    T = typing.TypeVar("T")


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


class _DbndScriptTrackingManager(object):
    def __init__(self):
        self._context_managers = []
        self._atexit_registered = False

        self._active = False

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

    def start(self, root_task_name=None, airflow_context=None):
        if self._run or self._active or try_get_databand_run():
            return

        # we probably should use only airlfow context via parameter.
        # also, there are mocks that cover only get_dbnd_project_config().airflow_context
        airflow_context = airflow_context or get_dbnd_project_config().airflow_context()
        set_tracking_config_overide(use_dbnd_log=True, airflow_context=airflow_context)

        dc = self._enter_cm(
            new_dbnd_context(name="inplace_tracking")
        )  # type: DatabandContext

        if airflow_context:
            root_task, job_name, source = build_run_time_airflow_task(airflow_context)
        else:
            root_task = _build_inline_root_task(root_task_name)
            job_name = root_task.task_name
            source = UpdateSource.dbnd

        self._run = run = self._enter_cm(
            new_databand_run(
                context=dc,
                job_name=job_name,
                existing_run=False,
                source=source,
                af_context=airflow_context,
            )
        )  # type: DatabandRun
        self._run.root_task = root_task

        if not self._atexit_registered:
            _set_process_exit_handler(self.stop)
            self._atexit_registered = True

        sys.excepthook = self.stop_on_exception
        self._active = True

        # now we send data to DB
        root_task_run = run._build_and_add_task_run(root_task)
        root_task_run.is_root = True

        # No need to track the state because we track in init_run
        run.root_task_run.set_task_run_state(TaskRunState.RUNNING, track=False)
        run.tracker.init_run()

        self._enter_cm(run.root_task_run.runner.task_run_execution_context())
        self._task_run = run.root_task_run

        return self._task_run

    def stop(self):
        if not self._active:
            return
        self._active = False
        try:
            databand_run = self._run
            root_tr = self._task_run
            root_tr.finished_time = utcnow()

            if root_tr.task_run_state not in TaskRunState.finished_states():
                for tr in databand_run.task_runs:
                    if tr.task_run_state == TaskRunState.FAILED:
                        root_tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED)
                        break
                else:
                    root_tr.set_task_run_state(TaskRunState.SUCCESS)

            if root_tr.task_run_state == TaskRunState.SUCCESS:
                databand_run.set_run_state(RunState.SUCCESS)
            else:
                databand_run.set_run_state(RunState.FAILED)

            # todo: hard to control the console output if we printing to the console not from the console tracker
            if not CoreConfig.current().silence_tracking_mode:
                logger.info(databand_run.describe.run_banner_for_finished())

            self._close_all_context_managers()

        except Exception as ex:
            _handle_tracking_error("dbnd-tracking-shutdown")

    def stop_on_exception(self, type, value, traceback):
        if self._active:
            try:
                error = TaskRunError.build_from_ex(
                    ex=value, task_run=self._task_run, exc_info=(type, value, traceback)
                )
                self._task_run.set_task_run_state(TaskRunState.FAILED, error=error)
            except:
                _handle_tracking_error("dbnd-set-script-error")

        self.stop()
        sys.__excepthook__(type, value, traceback)


def _set_process_exit_handler(handler):
    atexit.register(handler)

    # https://docs.python.org/3/library/atexit.html
    # The functions registered via this module are not called when the program
    # is killed by a signal not handled by Python, when a Python fatal internal
    # error is detected, or when os._exit() is called.
    #                       ^^^^^^^^^^^^^^^^^^^^^^^^^
    # and os._exit is the one used by airflow (and maybe other libraries)
    # so we'd like to monkey-patch os._exit to stop dbnd inplace run manager
    original_os_exit = os._exit

    def _dbnd_os_exit(*args, **kwargs):
        try:
            handler()
        finally:
            original_os_exit(*args, **kwargs)

    os._exit = _dbnd_os_exit


def _build_inline_root_task(root_task_name):
    # create "root task" with default name as current process executable file name
    if not root_task_name:
        root_task_name = sys.argv[0].split(os.path.sep)[-1]

    class InplaceTask(TrackingTask):
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

    root_task.ctrl.task_repr.task_command_line = list2cmdline(sys.argv)
    root_task.ctrl.task_repr.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)

    return root_task


def try_get_inplace_tracking_task_run():
    # type: ()->Optional[TaskRun]
    if get_dbnd_project_config().is_tracking_mode():
        return dbnd_run_start()


# there can be only one tracking manager
_dbnd_script_manager = None  # type: Optional[_DbndScriptTrackingManager]


def dbnd_run_start(name=None, airflow_context=None):
    dbnd_project_config = get_dbnd_project_config()
    if dbnd_project_config.disabled:
        return None

    global _dbnd_script_manager
    if not _dbnd_script_manager:
        dbnd_project_config._dbnd_tracking = True

        dsm = _DbndScriptTrackingManager()
        try:
            dsm.start(name, airflow_context)

            if dsm._active:
                _dbnd_script_manager = dsm
        except Exception as e:
            logger.error(e, exc_info=True)
            _handle_tracking_error("dbnd-tracking-start")
            dbnd_project_config.disabled = True
            return None
    if _dbnd_script_manager and _dbnd_script_manager._active:
        return _dbnd_script_manager._task_run


def dbnd_run_stop():
    global _dbnd_script_manager
    if _dbnd_script_manager:
        _dbnd_script_manager.stop()
        _dbnd_script_manager = None


@seven.contextlib.contextmanager
def dbnd_tracking(name=None, conf=None):
    # type: (...) -> TaskRun
    try:
        with config(config_values=conf, source="tracking context"):
            tr = dbnd_run_start(name=name)
            yield tr
    finally:
        dbnd_run_stop()


def _handle_tracking_error(msg):
    if is_verbose():
        logger.warning(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg,
            exc_info=True,
        )
    else:
        logger.info(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg
        )
