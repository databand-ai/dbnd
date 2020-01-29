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
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid


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
        in_memory=False,
        run_uid=None,
        airflow_context=False,
        job_name=None,
        sub_task_name=None,
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
            "task": {"task_in_memory_outputs": True},  # do not save any outputs
        }
        config.set_values(config_values=c, override=True, source="dbnd_start")
        context_kwargs = {"name": "airflow"} if airflow_context else {}
        # create databand context
        dc = self._enter_cm(new_dbnd_context(**context_kwargs))  # type: DatabandContext

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
                InplaceTask.task_definition.task_source_code = module_code
                InplaceTask.task_definition.task_module_code = module_code
        except Exception as ex:
            logger.info("Failed to find source code: %s", str(ex))

        root_task = InplaceTask(task_version="now", task_name=root_task_name)
        if airflow_context:
            root_task.task_is_system = True
        root_task.task_meta.task_command_line = list2cmdline(sys.argv)
        root_task.task_meta.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)

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

        if airflow_context and run_uid:
            # now create "main" task for current task_id task
            class InplaceSubTask(Task):
                _conf__task_family = sub_task_name

            task = InplaceSubTask(task_version="now", task_name=sub_task_name)
            tr = dr.create_dynamic_task_run(
                task, dr.local_engine, _uuid=get_task_run_uid(run_uid, sub_task_name)
            )
            self._start_taskrun(tr)

        return dr

    def _start_taskrun(self, task_run):
        self._enter_cm(task_run.runner.task_run_execution_context())
        task_run.start_time = utcnow()
        task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def stop(self, at_exit=True):
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


_dbnd_start_manager = _DbndInplaceRunManager()


def dbnd_run_start(name=None, in_memory=False):
    _dbnd_start_manager.start(root_task_name=name, in_memory=in_memory)


def dbnd_run_stop(at_exit=True):
    _dbnd_start_manager.stop(at_exit=at_exit)


def dbnd_run_start_airflow(dag_id=None, execution_date=None, task_id=None):
    if not dag_id:
        dag_id = os.environ["AIRFLOW_CTX_DAG_ID"]
    if not execution_date:
        execution_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"]
    if not task_id:
        task_id = os.environ["AIRFLOW_CTX_TASK_ID"]

    run_uid = get_job_run_uid(dag_id=dag_id, execution_date=execution_date)
    # root_task_uid = get_task_run_uid(run_uid=run_uid, task_id="DAG")
    # task_uid = get_task_run_uid(run_uid=run_uid, task_id=task_id)
    # this will create databand run with driver and root tasks.
    # we need the "root" task to be the same between different airflow tasks invocations
    # since in dbnd we must have single root task, so we create "dummy" task with dag_id name
    return _dbnd_start_manager.start(
        root_task_name="DAG",
        run_uid=run_uid,
        airflow_context=True,
        job_name=dag_id,
        sub_task_name=task_id,
    )


def apply_dbnd_task(task):
    pre_execute = task.pre_execute
    post_execute = task.post_execute

    def new_pre_execute(*args, **kwargs):
        pre_execute(*args, **kwargs)

        ti = kwargs["context"]["task_instance"]
        dbnd_run_start_airflow(
            dag_id=ti.dag_id, execution_date=ti.execution_date, task_id=ti.task_id
        )

    def new_post_execute(*args, **kwargs):
        dbnd_run_stop(at_exit=False)
        post_execute(*args, **kwargs)

    task.pre_execute = new_pre_execute
    task.post_execute = new_post_execute


def apply_dbnd(dag):
    for task in dag.tasks:
        apply_dbnd_task(task)
