import atexit
import logging
import os
import sys
import typing

from subprocess import list2cmdline

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import try_get_databand_context, try_get_databand_run
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_task_run_uid


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun

    T = typing.TypeVar("T")


class _DbndInplaceRunManager(object):
    def __init__(self):
        self._context_managers = []
        self._atexit_registered = False

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

    def start(
        self,
        root_task_name,
        in_memory=True,
        run_uid=None,
        airflow_context=False,
        job_name=None,
    ):
        if try_get_databand_context():
            return

        if not airflow_context and not self._atexit_registered:
            atexit.register(self.stop)
            if is_airflow_enabled():
                from airflow.settings import dispose_orm

                atexit.unregister(dispose_orm)
        c = {
            "run": {
                "skip_completed": False
            },  # we don't want to "check" as script is task_version="now"
            "task": {"task_in_memory_outputs": in_memory},  # do not save any outputs
        }
        config.set_values(config_values=c, override=True, source="dbnd_start")
        context_kwargs = {"name": "airflow"} if airflow_context else {}
        # create databand context
        dc = self._enter_cm(new_dbnd_context(**context_kwargs))  # type: DatabandContext

        root_task = _build_inline_root_task(
            root_task_name, airflow_context=airflow_context
        )
        # create databand run
        dr = self._enter_cm(
            new_databand_run(
                context=dc,
                task_or_task_name=root_task,
                run_uid=run_uid,
                existing_run=False,
                job_name=job_name,
            )
        )  # type: DatabandRun

        if run_uid:
            root_task_run_uid = get_task_run_uid(run_uid, root_task_name)
        else:
            root_task_run_uid = None
        dr._init_without_run(root_task_run_uid=root_task_run_uid)

        self._start_taskrun(dr.driver_task_run)
        self._start_taskrun(dr.root_task_run)
        return dr

    def _start_taskrun(self, task_run):
        self._enter_cm(task_run.runner.task_run_execution_context())
        task_run.start_time = utcnow()
        task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def stop(self, at_exit=True, update_run_state=True):
        if update_run_state:
            databand_run = try_get_databand_run()
            if databand_run:
                root_tr = databand_run.task.current_task_run
                root_tr.finished_time = utcnow()

                for tr in databand_run.task_runs:
                    if tr.task_run_state == TaskRunState.FAILED:
                        root_tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED)
                        databand_run.set_run_state(RunState.FAILED)
                        break
                else:
                    root_tr.set_task_run_state(TaskRunState.SUCCESS)
                    databand_run.set_run_state(RunState.SUCCESS)
                logger.info(databand_run.describe.run_banner_for_finished())

        self._close_all_context_managers()
        if at_exit and is_airflow_enabled():
            from airflow.settings import dispose_orm

            dispose_orm()


def _build_inline_root_task(root_task_name, airflow_context):
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
            if not airflow_context:
                # we don't set DAGs task's source code
                InplaceTask.task_definition.task_source_code = module_code
    except Exception as ex:
        logger.info("Failed to find source code: %s", str(ex))

    root_task = InplaceTask(task_version="now", task_name=root_task_name)

    if airflow_context:
        # we generate specific cmd values for airflow tasks in sync time
        root_task.task_is_system = True
        root_task.task_meta.task_command_line = ""
        root_task.task_meta.task_functional_call = ""
    else:
        root_task.task_meta.task_command_line = list2cmdline(sys.argv)
        root_task.task_meta.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)

    return root_task


_dbnd_start_manager = _DbndInplaceRunManager()


def get_dbnd_inplace_run_manager():
    return _dbnd_start_manager


def dbnd_run_start(name=None, in_memory=False):
    _dbnd_start_manager.start(root_task_name=name, in_memory=in_memory)


def dbnd_run_stop(at_exit=True, update_run_state=True):
    _dbnd_start_manager.stop(at_exit=at_exit, update_run_state=update_run_state)
