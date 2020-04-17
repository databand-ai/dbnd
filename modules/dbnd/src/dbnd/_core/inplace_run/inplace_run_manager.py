import atexit
import logging
import os
import sys
import typing

from subprocess import list2cmdline

from dbnd._core.configuration import environ_config
from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import is_verbose, try_get_databand_run
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.inplace_run.tracking_config import set_tracking_config_overide
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun

    T = typing.TypeVar("T")


def is_inplace_run():
    return environ_config.environ_enabled(environ_config.ENV_DBND__TRACKING)


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

            self._started = True

            # 1. create proper DatabandContext so we can create other objects
            set_tracking_config_overide(use_dbnd_log=True)
            # create databand context
            dc = self._enter_cm(new_dbnd_context())  # type: DatabandContext

            root_task = _build_inline_root_task(root_task_name)

            # create databand run
            self._run = self._enter_cm(
                new_databand_run(
                    context=dc,
                    task_or_task_name=root_task,
                    existing_run=False,
                    job_name=job_name,
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
        except Exception:
            _handle_inline_error("inline-start")
            self._disabled = True
            return
        finally:
            self._started = True

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
        except:
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


_dbnd_start_manager = _DbndInplaceRunManager()


def get_dbnd_inplace_run_manager():
    return _dbnd_start_manager


def dbnd_run_start(name=None):
    if not _dbnd_start_manager:
        return
    return _dbnd_start_manager.start(root_task_name=name)


def dbnd_run_stop():
    if not _dbnd_start_manager:
        return
    _dbnd_start_manager.stop()


def _handle_inline_error(msg):
    if is_verbose():
        logger.warning(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg,
            exc_info=True,
        )
    else:
        logger.info(
            "Failed during dbnd %s, ignoring, and continue without tracking" % msg
        )
