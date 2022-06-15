import atexit
import logging
import os
import sys
import typing

from subprocess import list2cmdline
from typing import Optional

import dbnd

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.config_value import ConfigValuePriority
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import try_get_script_name
from dbnd._core.constants import RunState, TaskRunState, UpdateSource
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import try_get_databand_run
from dbnd._core.log.config import FORMAT_SIMPLE
from dbnd._core.parameter.parameter_value import Parameters
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.settings import TrackingConfig
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_source_code import TaskSourceCode
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    build_run_time_airflow_task,
    override_airflow_log_system_for_tracking,
)
from dbnd._core.tracking.managers.callable_tracking import _handle_tracking_error
from dbnd._core.tracking.schemas.tracking_info_run import RootRunInfo
from dbnd._core.utils import seven
from dbnd._core.utils.airflow_utils import get_project_name_from_airflow_tags
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid
from dbnd._vendor import pendulum


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun

    T = typing.TypeVar("T")


# Helper functions


def _set_tracking_config_overide(airflow_context=None):
    # Ceate proper DatabandContext so we can create other objects
    # There should be no Orchestrations tasks.
    # However, let's disable any orchestrations side effects
    config_for_tracking = {
        "run": {
            "skip_completed": False,
            "skip_completed_on_run": False,
            "validate_task_inputs": False,
            "validate_task_outputs": False,
        },  # we don't want to "check" as script is task_version="now"
        "task": {"task_in_memory_outputs": True},  # do not save any outputs
        "core": {"tracker_raise_on_error": False},  # do not fail on tracker errors
    }
    if airflow_context:
        import pytz

        task_target_date = pendulum.parse(
            airflow_context.execution_date, tz=pytz.UTC
        ).date()
        use_dbnd_log = override_airflow_log_system_for_tracking()
        if use_dbnd_log is not None:
            config_for_tracking["log"] = {"disabled": not use_dbnd_log}

        config_for_tracking["task"]["task_target_date"] = task_target_date

    return config.set_values(
        config_values=config_for_tracking,
        priority=ConfigValuePriority.OVERRIDE,
        source="dbnd_tracking_config",
    )


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

    task_definition = TaskDefinition(
        task_passport=TaskPassport.from_module(
            TrackingTask.__module__
        ),  # we need to fix that
        source_code=TaskSourceCode.from_callstack(),
    )

    root_task = TrackingTask(
        task_name=root_task_name,
        task_definition=task_definition,
        task_params=Parameters(source="inline_root_task", param_values=[]),
    )

    root_task.ctrl.task_repr.task_command_line = list2cmdline(sys.argv)
    root_task.ctrl.task_repr.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)

    return root_task


def _set_dbnd_config_from_airflow_connections():
    """Set Databand config from Extra section in Airflow dbnd_config connection."""
    try:
        from dbnd_airflow.tracking.dbnd_airflow_conf import (
            set_dbnd_config_from_airflow_connections,
        )

        set_dbnd_config_from_airflow_connections()

    except ImportError:
        logger.info(
            "dbnd_airflow is not installed. Config will not load from Airflow Connections"
        )


def has_level_handler(loggr: logging.Logger):
    """Check if there is a handler in the logging chain that will handle the
    given logger's :meth:`effective level <~logging.Logger.getEffectiveLevel>`.

    Arguments:
        loggr : the logger to inspect
    """
    level = loggr.getEffectiveLevel()
    current = loggr

    while current:
        if any(handler.level <= level for handler in current.handlers):
            return True

        if not current.propagate:
            break

        current = current.parent

    return False


def _configure_tracking_logging(conf):
    dbnd_logger = logging.getLogger("dbnd")
    logging_level = conf["log"].get("level", "WARNING")
    dbnd_logger.setLevel(logging_level)
    if not has_level_handler(dbnd_logger):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(FORMAT_SIMPLE))
        dbnd_logger.addHandler(console_handler)


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

    def update_run_from_airflow_context(self, airflow_context):
        if not airflow_context or not airflow_context.context:
            return

        dag = airflow_context.context.get("dag", None)
        if not dag:
            return

        dag_tags = getattr(dag, "tags", [])
        project_name = get_project_name_from_airflow_tags(dag_tags)
        airflow_user = airflow_context.context["dag"].owner

        if project_name:
            self._run.project_name = project_name

        if airflow_user:
            self._run.context.task_run_env.user = airflow_user

        if airflow_context.is_subdag:
            root_run_uid = get_job_run_uid(
                airflow_instance_uid=airflow_context.airflow_instance_uid,
                dag_id=airflow_context.root_dag_id,
                execution_date=airflow_context.execution_date,
            )
            self._run.root_run_info = RootRunInfo(
                root_run_uid=root_run_uid,
                root_task_run_uid=get_task_run_uid(
                    run_uid=root_run_uid,
                    dag_id=airflow_context.root_dag_id,
                    task_id=airflow_context.dag_id.split(".")[-1],
                ),
            )

    def start(self, root_task_name=None, project_name=None, airflow_context=None):
        if self._run or self._active or try_get_databand_run():
            return

        # we probably should use only airlfow context via parameter.
        # also, there are mocks that cover only get_dbnd_project_config().airflow_context
        airflow_context = airflow_context or get_dbnd_project_config().airflow_context()
        if airflow_context:
            _set_dbnd_config_from_airflow_connections()

        _set_tracking_config_overide(airflow_context=airflow_context)
        dc = self._enter_cm(
            new_dbnd_context(name="inplace_tracking")
        )  # type: DatabandContext

        if not root_task_name:
            # extract the name of the script we are running (in Airflow scenario it will be just "airflow")
            root_task_name = sys.argv[0].split(os.path.sep)[-1]

        if airflow_context:
            root_task, job_name, source, run_uid = build_run_time_airflow_task(
                airflow_context, root_task_name
            )
            try_number = airflow_context.try_number
        else:
            root_task = _build_inline_root_task(root_task_name)
            job_name = root_task_name
            source = UpdateSource.generic_tracking
            run_uid = None
            try_number = 1

        tracking_source = (
            None  # TODO_CORE build tracking_source -> typeof TrackingSourceSchema
        )
        self._run = run = self._enter_cm(
            new_databand_run(
                context=dc,
                job_name=job_name,
                run_uid=run_uid,
                existing_run=run_uid is not None,
                source=source,
                af_context=airflow_context,
                tracking_source=tracking_source,
                project_name=project_name,
            )
        )  # type: DatabandRun

        self._run.root_task = root_task

        self.update_run_from_airflow_context(airflow_context)

        if not self._atexit_registered:
            _set_process_exit_handler(self.stop)
            self._atexit_registered = True

        sys.excepthook = self.stop_on_exception
        self._active = True

        # now we send data to DB
        root_task_run = run._build_and_add_task_run(
            root_task, task_af_id=root_task.task_name, try_number=try_number
        )

        root_task_run.is_root = True

        run.tracker.init_run()
        run.root_task_run.set_task_run_state(TaskRunState.RUNNING)

        should_capture_log = TrackingConfig.from_databand_context().capture_tracking_log
        self._enter_cm(
            run.root_task_run.runner.task_run_execution_context(
                capture_log=should_capture_log, handle_sigterm=False
            )
        )
        self._task_run = run.root_task_run

        return self._task_run

    def stop(self, finalize_run=True):
        if not self._active:
            return
        self._active = False
        try:
            # Required for scripts tracking which do not set the state to SUCCESS
            if finalize_run:
                databand_run = self._run
                root_tr = self._task_run
                root_tr.finished_time = utcnow()

                if root_tr.task_run_state not in TaskRunState.finished_states():
                    for tr in databand_run.task_runs:
                        if tr.task_run_state == TaskRunState.FAILED:
                            root_tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED)
                            break
                    else:
                        # We can reach here in case of raising exception tracking stand alone python script
                        if sys.exc_info()[1]:
                            error = TaskRunError.build_from_ex(None, root_tr)
                            root_tr.set_task_run_state(TaskRunState.FAILED, error=error)
                        else:
                            root_tr.set_task_run_state(TaskRunState.SUCCESS)

                if root_tr.task_run_state == TaskRunState.SUCCESS:
                    databand_run.set_run_state(RunState.SUCCESS)
                else:
                    databand_run.set_run_state(RunState.FAILED)

            self._close_all_context_managers()

        except Exception:
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


# API functions


def try_get_inplace_tracking_task_run():
    # type: ()->Optional[TaskRun]
    if get_dbnd_project_config().is_tracking_mode():
        return dbnd_tracking_start()


# there can be only one tracking manager
_dbnd_script_manager = None  # type: Optional[_DbndScriptTrackingManager]


def tracking_start_base(job_name, project_name=None, airflow_context=None):
    """
    Starts handler for tracking the current running script.
    Would not start a new one if script manager if already exists
    """
    dbnd_project_config = get_dbnd_project_config()
    if dbnd_project_config.disabled:
        # we are not tracking if dbnd is disabled
        return None

    global _dbnd_script_manager
    if not _dbnd_script_manager:
        # setting the context to tracking to prevent conflicts from dbnd orchestration
        dbnd_project_config._dbnd_tracking = True

        dsm = _DbndScriptTrackingManager()
        try:
            # we use job name for both job_name and root_task_name of the run
            dsm.start(job_name, project_name, airflow_context)
            if dsm._active:
                _dbnd_script_manager = dsm

        except Exception:
            _handle_tracking_error("dbnd-tracking-start")

            # disabling the project so we don't start any new handler in this execution
            dbnd_project_config.disabled = True
            return None

    if _dbnd_script_manager and _dbnd_script_manager._active:
        # this is the root task run of the tracking, its representing the script context.
        return _dbnd_script_manager._task_run


def dbnd_airflow_tracking_start(airflow_context):
    """This function is meant to be used only from inside Airflow for Airflow tracking only."""
    job_name = try_get_script_name()
    return tracking_start_base(job_name=job_name, airflow_context=airflow_context)


def dbnd_tracking_start(job_name=None, run_name=None, project_name=None, conf=None):
    """
    This function is used for tracking Python scripts only and should be added at the beginning of the script.

    When the script execution ends, dbnd_tracking_stop will be called automatically, there is no need to add it manually.

    Args:
        job_name: Name of the pipeline
        run_name: Name of the run
        project_name: Name of the project
        conf: Configuration dict with values for Databand configurations
    """
    if not conf:
        conf = {}

    if run_name:
        conf.setdefault("run", {}).setdefault("name", run_name)

    # do not apply our logger to a python script by default
    conf.setdefault("log", {}).setdefault("disabled", True)

    # send logs to webserver by default
    conf.setdefault("tracking", {}).setdefault("capture_tracking_log", True)

    if conf["log"]["disabled"]:
        _configure_tracking_logging(conf)

    # We use print here and not log because the dbnd logger might be set to Warning (by default), and we want to
    # inform the user that we started, without alerting him with a Warning or Error message.
    # This should be a logger info message when tracking and orchestration split.
    print("Databand Tracking Started {version}".format(version=dbnd.__version__))

    if conf:
        config.set_values(
            config_values=conf,
            priority=ConfigValuePriority.OVERRIDE,
            source="dbnd_tracking_start",
        )

    if job_name is None:
        job_name = try_get_script_name()

    return tracking_start_base(job_name=job_name, project_name=project_name)


def dbnd_tracking_stop(finalize_run=True):
    """
    Stops and clears the script tracking if exists.

    Args:
        finalize_run: Should complete the run by setting it's state to a complete one (success or failed).
    """
    global _dbnd_script_manager
    if _dbnd_script_manager:
        _dbnd_script_manager.stop(finalize_run)
        _dbnd_script_manager = None


@seven.contextlib.contextmanager
def dbnd_tracking(job_name=None, run_name=None, project_name=None, conf=None):
    # type: (...) -> TaskRun
    """
    This function is used for tracking Python scripts only and should be used with a with statement.

    Args:
        job_name: Name of the pipeline
        run_name: Name of the run
        project_name: Name of the project
        conf: Configuration dict with values for Databand configurations
    """
    try:
        tr = dbnd_tracking_start(
            job_name=job_name, run_name=run_name, project_name=project_name, conf=conf
        )
        yield tr
    finally:
        dbnd_tracking_stop()
