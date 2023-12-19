# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import os
import sys
import threading
import typing

from typing import Iterator, Optional, Union
from uuid import UUID

import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import (
    ENV_DBND__USER_PRE_INIT,
    is_unit_test_mode,
)
from dbnd._core.constants import (
    RunState,
    SystemTaskName,
    TaskExecutorType,
    TaskRunState,
)
from dbnd._core.context.use_dbnd_run import is_dbnd_orchestration_via_airflow_enabled
from dbnd._core.current import current_task_run
from dbnd._core.errors import DatabandRunError, DatabandSystemError
from dbnd._core.errors.base import (
    DatabandError,
    DatabandFailFastError,
    DatabandSigTermError,
    DbndCanceledRunError,
)
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd._core.utils import console_utils
from dbnd._core.utils.basics.load_python_module import (
    load_python_callable,
    run_user_func,
)
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.pycharm_debugger import start_pycharm_debugger
from dbnd._core.utils.seven import cloudpickle
from dbnd._core.utils.task_utils import get_project_name_safe, get_task_name_safe
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.traversing import flatten
from dbnd._core.utils.uid_utils import get_uuid
from dbnd.api.runs import kill_run
from dbnd_run.plugin.dbnd_plugins import pm
from dbnd_run.run_executor.factory import (
    calculate_task_executor_type,
    get_task_executor,
)
from dbnd_run.run_executor.heartbeat_sender import start_heartbeat_sender
from dbnd_run.run_executor.results_view import RunResultBand
from dbnd_run.run_executor.task_runs_builder import TaskRunsBuilder
from dbnd_run.run_executor_engine.local_task_executor import (
    LocalTaskExecutor,
    topological_sort,
)
from dbnd_run.run_settings import EnvConfig, RunLoggingConfig, RunSettings
from dbnd_run.run_settings.engine import build_engine_config
from dbnd_run.run_settings.run import RunConfig
from dbnd_run.task.task import Task
from dbnd_run.task_ctrl.task_dag_describe import print_tasks_tree
from dbnd_run.task_ctrl.task_run_executor import TaskRunExecutor
from targets import FileTarget, target
from targets.caching import TARGET_CACHE
from targets.providers.pandas import register_pd_to_hdf5_as_table_marshaler


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext

DEFAULT_TASK_CANCELED_ERR_MSG = "Task was killed by the user"

logger = logging.getLogger(__name__)

# naive implementation of stop event
# we can't save it on Context (non pickable in some cases like running in multithread python)
# if somebody is killing run it's global for the whole process
_is_killed = threading.Event()


class RunExecutor(SingletonContext):
    """
    This class is in charge of running the pipeline at orchestration (dbnd run) mode
    It wraps it's own execution with _RunExecutor_Task task, so logs and state are reported to tracker
    * in submitter mode - send same command to remote engine
    * in driver mode - build and run the task
    """

    def __init__(
        self, run, root_task_or_task_name, send_heartbeat, force_task_name=None
    ):

        self.run = run  # type: DatabandRun
        self.databand_context = run.context  # type: DatabandContext
        self.send_heartbeat = send_heartbeat

        if root_task_or_task_name is None:
            raise DatabandSystemError(
                "Run executor requires task name or task, got None"
            )

        # we are building it only in driver,
        # root task sometimes can be executed only inside the docker
        # it can be affected by docker env (k8s secrets/external source)
        self.root_task_name_to_build = None
        self.force_task_name = force_task_name
        if isinstance(root_task_or_task_name, six.string_types):
            self.root_task_name_to_build = root_task_or_task_name
        elif isinstance(root_task_or_task_name, Task):
            # we have a ready task, we will not build it, just run
            self.run.root_task = root_task_or_task_name
        else:
            raise DatabandSystemError(
                "Run executor requires task name or task, got %s - %s",
                root_task_or_task_name,
                type(root_task_or_task_name),
            )

        self.run_folder_prefix = os.path.join(
            "log",
            run.execution_date.strftime("%Y-%m-%d"),
            "%s_%s_%s"
            % (
                run.execution_date.strftime("%Y-%m-%dT%H%M%S.%f"),
                run.job_name,
                run.name,
            ),
        )
        self.run_root = self.env.dbnd_root.folder(self.run_folder_prefix)
        self.run_local_root = self.env.dbnd_local_root.folder(self.run_folder_prefix)

        self.driver_dump = self.run_root.file("run.pickle")

        self.local_engine = build_engine_config(self.env.local_engine)
        self.remote_engine = build_engine_config(
            self.env.remote_engine or self.env.local_engine
        )

        # we take values from run config (if defined) - otherwise from specific env definition
        # usually run_config will contain "negative" override
        # values at env_config are based on env.remote_config ( try to submit_driver/submit_tasks if defined)
        self.submit_driver = (
            self.run_config.submit_driver
            if self.run_config.submit_driver is not None
            else self.env.submit_driver
        )
        self.submit_tasks = (
            self.run_config.submit_tasks
            if self.run_config.submit_tasks is not None
            else self.env.submit_tasks
        )
        self.task_executor_type, self.parallel = calculate_task_executor_type(
            self.submit_tasks, self.remote_engine, self.run_config
        )

        self.local_engine = build_engine_config(self.env.local_engine).clone(
            require_submit=False
        )
        if self.submit_driver and not run.existing_run:
            # we are running submitter, that will send driver to remote
            self.run_executor_type = SystemTaskName.driver_submit
            self.host_engine = self.local_engine
        else:
            # We are running Driver ( submitter already sent this , or no submitter at all)
            self.run_executor_type = SystemTaskName.driver
            if self.submit_driver:
                # submit drive is true, but we are in existing run:
                # we are after the jump from submit to driver execution (to remote engine)
                self.host_engine = self.remote_engine
            else:
                self.host_engine = self.local_engine
            if not self.submit_tasks or self.task_executor_type == "airflow_kubernetes":
                # if we are not in submit tasks, we disable engine "resubmit"
                # airflow kubernetes - we don't want task resubmition, even if engine is k8s
                self.remote_engine = self.remote_engine.clone(require_submit=False)
        # we are running at this engine already
        self.host_engine = self.host_engine.clone(require_submit=False)

        # dag_id , execution_date are used by Airflow,
        # should be moved to this class (still used by DB tracking)
        # run.dag_id = AD_HOC_DAG_PREFIX + run.job_name

        self._result_location = None
        self.runtime_errors = []

        # we are running from python notebook, let start to print to stdout
        if run.context.name == "interactive" or is_unit_test_mode():
            run.context.config.set(
                RunLoggingConfig._conf__task_family,
                "stream_stdout",
                "True",
                source="log",
            )

        self._is_initialized = False

    @property
    def run_settings(self) -> RunSettings:
        return self.databand_context.run_settings

    @property
    def run_config(self) -> RunConfig:
        return self.run_settings.run

    @property
    def env(self) -> EnvConfig:
        return self.run_settings.env

    def _on_enter(self):
        pm.hook.dbnd_on_pre_init_context(ctx=self)
        run_user_func(config.get("core", "user_pre_init"))
        # if we are deserialized - we don't need to run this code again.
        run_config = self.run_config
        if not self._is_initialized:
            # will be called from singleton context manager

            pm.hook.dbnd_on_new_context(ctx=self)

            # RUN USER SETUP FUNCTIONS
            _run_user_func(
                run_config.__class__.user_driver_init, run_config.user_driver_init
            )

            self._is_initialized = True
        else:
            # we get here if we are running at sub process that recreates the Context
            pm.hook.dbnd_on_existing_context(ctx=self)

        # we do it every time we go into databand_config
        self.configure_targets()

        self.run_settings.run_logging.configure_dbnd_logging()

        _run_user_func(run_config.__class__.user_init, run_config.user_init)
        pm.hook.dbnd_post_enter_context(ctx=self)

    def _on_exit(self):
        pm.hook.dbnd_on_exit_context(ctx=self)

    def configure_targets(self):
        output_config = self.run_settings.output
        if output_config.hdf_format == "table":
            register_pd_to_hdf5_as_table_marshaler()

    def is_interactive(self):
        return self.name == "interactive"

    def run_execute(self):
        """
        Runs the main driver!
        """
        run = self.run

        # NO CHANGES TO GLOBAL CONFIG AFTER THIS POINT,
        # run_executor__task will keep the config it was created with, so we will have old config durign .run()
        self.run_executor__task = _RunExecutor_Task(
            task_name=self.run_executor_type, task_version=run.run_uid
        )
        if self.run.root_task:
            # if root_task == None, we will create it in the context of driver task
            # otherwise, we need it to add manually
            self.run_executor__task.descendants.add_child(run.root_task.task_id)

        # will track and "execute" pipeline
        # you need to run DatabandRun.init_run otherwise it will be not tracked
        self.run._driver_task_run = self.run.build_task_run(
            task=self.run_executor__task
        )
        self.build_task_run_executor(
            self.run._driver_task_run, task_engine=self.host_engine
        )

        run.tracker.init_run()
        run.set_run_state(RunState.RUNNING)

        # with captures_log_into_file_as_task_file(log_file=self.local_driver_log.path):
        try:
            if self.run_config.debug_pydevd_pycharm_port is not None:
                debug_port = self.run_config.debug_pydevd_pycharm_port
                # currently no need to export the host to configuration.
                start_pycharm_debugger(host="localhost", port=debug_port)

            self.start_time = utcnow()
            run.driver_task_run.task_run_executor.execute()
            self.finished_time = utcnow()
            # if we are in submitter we don't want to print banner that everything is good
        except DatabandRunError as ex:
            self._dbnd_run_error(ex)
            raise
        except (Exception, KeyboardInterrupt, SystemExit) as ex:
            raise self._dbnd_run_error(ex)
        finally:
            try:
                self.host_engine.cleanup_after_run()
            except Exception:
                logger.exception("Failed to shutdown the current run, continuing")

        return self

    def get_current_dbnd_local_root(self):
        # we should return here the proper engine config, based in which context we run right now
        # it could be submit, driver or task engine
        return self.env.dbnd_local_root

    def _dbnd_run_error(self, ex):
        if (
            # what scenario is this aiflow filtering supposed to help with?
            # I had airflow put a default airflow.cfg in .dbnd causing validation error in k8sExecutor which was invisible in the console (only in task log)
            (
                "airflow" not in ex.__class__.__name__.lower()
                or ex.__class__.__name__ == "AirflowConfigException"
            )
            and "Failed tasks are:" not in str(ex)
            and not isinstance(ex, DatabandRunError)
            and not isinstance(ex, KeyboardInterrupt)
            and not isinstance(ex, DatabandSigTermError)
        ):
            logger.exception(ex)

        if (
            isinstance(ex, KeyboardInterrupt)
            or isinstance(ex, DatabandSigTermError)
            or self.is_killed()
        ):
            run_state = RunState.CANCELLED
            unfinished_task_state = TaskRunState.UPSTREAM_FAILED
        elif isinstance(ex, DatabandFailFastError):
            run_state = RunState.FAILED
            unfinished_task_state = TaskRunState.UPSTREAM_FAILED
        else:
            run_state = RunState.FAILED
            unfinished_task_state = TaskRunState.FAILED

        self.run.set_run_state(run_state)
        self.run.tracker.tracking_store.set_unfinished_tasks_state(
            run_uid=self.run.run_uid, state=unfinished_task_state
        )

        logger.warning(
            "\n\n{sep}\n{banner}\n{sep}".format(
                sep=console_utils.error_separator(),
                banner=self.run.describe.run_banner(
                    "Your run has failed! See more info above.",
                    color="red",
                    show_run_info=True,
                    show_tasks_info=True,
                ),
            )
        )
        return DatabandRunError(
            "Run has failed: %s" % ex, run=self.run, nested_exceptions=ex
        )

    def _is_save_run_pickle(self, task_runs, remote_engine):
        """we need to save run pickle only if we want to execute it in the remote process"""
        if self.run_config.always_save_pipeline:
            return True
        if self.run_config.disable_save_pipeline:
            return False

        if any(tr.task._conf__require_run_dump_file for tr in task_runs):
            return True

        if remote_engine.require_submit:
            return True

        if self.task_executor_type == TaskExecutorType.local:
            return False

        if is_dbnd_orchestration_via_airflow_enabled():
            from dbnd_run.airflow.executors import AirflowTaskExecutorType

            return self.task_executor_type not in [
                AirflowTaskExecutorType.airflow_inprocess,
                TaskExecutorType.local,
            ]
        return True

    def save_run_pickle(self, target_file=None):
        """
        dumps current run and context to file
        """
        t = target_file or self.driver_dump
        logger.info("Saving current run into %s", t)

        # Ensure tracking is completed before pickling
        self.run.tracker.tracking_store.flush()

        with t.open("wb") as fp:
            cloudpickle.dump(obj=self.run.run_executor, file=fp)

    @classmethod
    def load_run(cls, dump_file, disable_tracking_api):
        # type: (FileTarget, bool) -> RunExecutor
        logger.info("Loading dbnd run execution from %s", dump_file)
        with dump_file.open("rb") as fp:
            run_executor: RunExecutor = cloudpickle.load(file=fp)
            if disable_tracking_api:
                run_executor.run_config.context.tracking_store.disable_tracking_api()
                logger.info("Tracking has been disabled")
        try:
            if run_executor.run_config.pickle_handler:
                pickle_handler = load_python_callable(
                    run_executor.run_config.pickle_handler
                )
                pickle_handler(run_executor)
        except Exception as e:
            logger.exception(
                "error while trying to handle pickle with custom handler:", e
            )
        return run_executor

    def is_killed(self):
        return _is_killed.is_set()

    def _internal_kill(self):
        """
        called by TaskRun handler, so we know that run is "canceled"
        otherwise we will get regular exception
        """
        _is_killed.set()

    def kill(self):
        """
        called from user space, kills the current task only
        :return:
        """
        # this is very naive stop implementation
        # in case of simple executor, we'll run task.on_kill code
        _is_killed.set()
        try:
            current_task = None
            from dbnd._core.task_build.task_context import TaskContext, TaskContextPhase

            tc = TaskContext.try_instance()
            if tc.phase == TaskContextPhase.RUN:
                current_list = list(tc.stack)
                if current_list:
                    current_task = current_list.pop()
        except Exception as ex:
            logger.warning("Failed to find current task: %s" % ex)
            return

        if not current_task:
            logger.info("No current task.. Killing nothing..")
            return

        try:
            current_task.on_kill()
        except Exception as ex:
            logger.warning("Failed to kill current task %s: %s" % (current_task, ex))
            return

    def kill_run(self, message=None):
        _is_killed.set()

        # When initiating kill_run, the api's kill_run sends a signal to all running runs,
        # to change their state to shutdown, which in the end sets it to cancelled.
        # the task which initiated the killing, the current task run, should have a state of Failed, and not Canceled.
        # It is important to set it with an error, to allow the passing of error message, to be displayed in the UI
        # as the error_message for the whole run.
        tr = current_task_run()
        if tr.run == self.run:
            task_run_error = TaskRunError.build_from_message(
                task_run=tr,
                msg=message or DEFAULT_TASK_CANCELED_ERR_MSG,
                help_msg="task with task_run_uid:%s initiated kill_run"
                % (tr.task_run_uid),
                ex_class=DbndCanceledRunError,
            )
            tr.set_task_run_state(TaskRunState.FAILED, track=True, error=task_run_error)
        try:
            kill_run(str(self.run.run_uid), ctx=self.run.context)
        except Exception as e:
            raise DatabandFailFastError(
                "Could not send request to kill databand run!", e
            )
        if tr.run == self.run:
            raise DatabandError(message or DEFAULT_TASK_CANCELED_ERR_MSG)

    # all paths, we make them system, we don't want to check if they are exists

    def _init_task_runs_for_execution(self, task_engine):
        """
        creates all relevant task runs starting root_task (driver is not part of this logic)
        we run it from Driver, so this process is logged via _executor task
        """
        run = self.run
        task = run.root_task

        task_runs = TaskRunsBuilder().build_task_runs(
            self, task, task_run_engine=task_engine
        )

        # this one will add tasks to the run! track task runs
        run.tracker.add_task_runs(task_runs)

        return task_runs

    def run_task_at_execution_time(self, task, task_engine=None):
        if task_engine is None:
            task_engine = self.host_engine
        task_run = self.run.build_task_run_and_track(task)
        task_run_executor = self.build_task_run_executor(task_run, task_engine)
        task_run_executor.execute()
        return task_run

    def build_task_run_executor(self, task_run, task_engine):
        task_run_executor = TaskRunExecutor(
            task_run=task_run, run_executor=self, task_engine=task_engine
        )
        task_run.task_run_executor = task_run_executor
        return task_run_executor

    def run_driver(self):
        logger.info("Running driver... Driver PID: %s", os.getpid())

        run = self.run  # type: DatabandRun
        run_executor = run.run_executor
        run_settings = run_executor.run_settings
        remote_engine = run_executor.remote_engine

        run_settings.git.validate_git_policy()
        # let prepare for remote execution
        remote_engine.prepare_for_run(run)

        if self.root_task_name_to_build:

            if self.force_task_name:
                kwargs = {"task_name": self.force_task_name}

                logger.info(
                    "Building main task '%s' with name %s",
                    self.root_task_name_to_build,
                    self.force_task_name,
                )
            else:
                logger.info("Building main task '%s'", self.root_task_name_to_build)
                kwargs = {}
            root_task = get_task_registry().build_dbnd_task(
                self.root_task_name_to_build, task_kwargs=kwargs
            )
            logger.info(
                "Task %s has been created (%s children)",
                root_task.task_id,
                len(root_task.ctrl.task_dag.subdag_tasks()),
            )
            run.root_task = root_task

        tasks = run.root_task.task_dag.subdag_tasks()
        # assert that graph is DAG
        topological_sort(tasks)

        # now we init all task runs for all tasks in the pipeline
        task_runs = self._init_task_runs_for_execution(task_engine=remote_engine)
        root_task_run = run.root_task_run
        run.root_task.ctrl.banner(
            "Main task '%s' has been created!" % root_task_run.task_af_id,
            color="cyan",
            task_run=root_task_run,
        )

        if self.run_config.dry:
            run.root_task.ctrl.describe_dag.describe_dag()
            logger.warning("Execution has been stopped due to run.dry=True flag!")
            return run

        print_tasks_tree(root_task_run.task, task_runs)
        if self._is_save_run_pickle(task_runs, remote_engine):
            run_executor.save_run_pickle()

        task_runs_to_run = [tr for tr in task_runs if not tr.is_skipped]

        # THIS IS THE POINT WHEN WE SUBMIT ALL TASKS TO EXECUTION
        # we should make sure that we create executor without driver task
        task_executor = get_task_executor(
            self,
            task_executor_type=run_executor.task_executor_type,
            host_engine=run_executor.host_engine,
            target_engine=remote_engine,
            task_runs=task_runs_to_run,
        )

        hearbeat = None
        if self.send_heartbeat:
            # this will wrap the executor with "heartbeat" process
            hearbeat = start_heartbeat_sender(self)

        with nested(hearbeat):
            task_executor.do_run()

        # We need place the pipeline's task_band in the place we required to by outside configuration
        if run_settings.run.run_result_json_path:
            new_path = run_settings.run.run_result_json_path
            try:
                self.result_location.copy(new_path)
            except Exception as e:
                logger.exception(
                    "Couldn't copy the task_band from {old_path} to {new_path}. Failed with this error: {error}".format(
                        old_path=self.result_location.path, new_path=new_path, error=e
                    )
                )

            else:
                logger.info(
                    "Copied the pipeline's task_band to {new_path}".format(
                        new_path=new_path
                    )
                )

        # if we are in the driver, we want to print banner after executor__task banner
        run.set_run_state(RunState.SUCCESS)

        root_task = self.run.root_task_run.task
        msg = "Your run has been successfully executed!"
        if self.run.duration:
            msg = "Your run has been successfully executed in %s" % self.run.duration
        run_msg = "\n%s\n%s\n" % (
            root_task.ctrl.banner(
                "Main task '%s' is ready!" % root_task.task_name,
                color="green",
                task_run=self.run.root_task_run,
            ),
            run.describe.run_banner(msg, color="green", show_tasks_info=True),
        )
        logger.info(run_msg)

        return run

    def run_submitter(self):
        """
        This is the task that represents "submission"
        it can just one task, or.. more tasks, as we can have "docker builds" or other preparations
        this is why we will not run it directly, but do a "full run" with executor

        """
        # we are running submitter, that will send driver to remote
        self.run_settings.git.validate_git_policy()

        run = self.run
        # let prepare for remote execution
        remote_engine = self.remote_engine
        remote_engine.prepare_for_run(run)

        result_map_target = self.run_root.file("{}.json".format(get_uuid()))
        cmd_line_args = (
            ["run"]
            + _get_dbnd_run_relative_cmd()
            + [
                "--run-driver",
                str(run.run_uid),
                "--set",
                "run.run_result_json_path={}".format(result_map_target.path),
                "--set",
                "run_info.execution_date={}".format(
                    run.execution_date.strftime("%Y-%m-%dT%H%M%S.%f")
                ),
            ]
        )

        args = remote_engine.dbnd_executable + cmd_line_args
        submit_to_engine_task = remote_engine.submit_to_engine_task(
            env=run.run_executor.env,
            args=args,
            task_name="dbnd_driver_run",
            interactive=self.run_config.interactive,
        )
        submit_to_engine_task._conf_confirm_on_kill_msg = (
            "Ctrl-C Do you want to kill your submitted pipeline?"
            "If selection is 'no', this process will detach from the run."
        )
        run.root_task = submit_to_engine_task

        # we run all tasks on local engine
        task_runs = self._init_task_runs_for_execution(task_engine=self.host_engine)

        # create executor without driver task!
        # We use local executor to run all tasks (submit_to_engine and required by it tasks)
        # In most cases it will run only submit_to_engine task,
        # But there are scenarios when submit_to_engine task asks for docker builds
        # so we execute the whole pipeline.
        task_executor = LocalTaskExecutor(
            run_executor=self,
            task_executor_type=TaskExecutorType.local,
            host_engine=self.host_engine,
            target_engine=self.host_engine,
            task_runs=task_runs,
        )

        task_executor.do_run()
        self.result_location = result_map_target

        logger.info(run.describe.run_banner_for_submitted())

    def cleanup_after_task_run(self, task):
        # type: (Task) -> None
        rels = task.ctrl.relations
        # potentially, all inputs/outputs targets for current task could be removed
        targets_to_clean = set(flatten([rels.task_inputs, rels.task_outputs]))

        targets_in_use = set()
        # any target which appears in inputs of all not finished tasks shouldn't be removed
        for tr in self.run.task_runs:
            if tr.task_run_state in TaskRunState.final_states():
                continue
            # remove all still needed inputs from targets_to_clean list
            for target in flatten(tr.task.ctrl.relations.task_inputs):
                targets_in_use.add(target)

        TARGET_CACHE.clear_for_targets(targets_to_clean - targets_in_use)

    def get_upstream_failed_task_run_state(self, task_run: "TaskRun") -> "TaskRunState":
        """
        This function is a helper function to decide task final state only for orchestration runs.
        param: task_run_executor - the task  which we want to get it's state
        return: TaskRunState
        """
        state = TaskRunState.UPSTREAM_FAILED
        child_task_runs = task_run.task.descendants
        # Since task run has children, IT'S a pipeline task, so we need to calculate it's state based on children states
        if child_task_runs:
            for task_run_id in child_task_runs.children:
                child_tr_instance = self.run.get_task_run_by_id(task_run_id)
                if child_tr_instance.task_run_state == TaskRunState.FAILED:
                    # In case one of its children fail,task run state is force updated from upstream failed to failed
                    #   because upstream_failed means task dependency is not met, while actually inner task has errors
                    state = TaskRunState.FAILED

        return state

    def get_context_spawn_env(self):
        env = self.run.get_context_spawn_env()
        if self.run_config.user_code_on_fork:
            env[ENV_DBND__USER_PRE_INIT] = self.run_config.user_code_on_fork
        return env

    @property
    def result(self):
        # type: () -> RunResultBand
        return RunResultBand.from_target(self.result_location)

    @property
    def result_location(self):
        # type: () -> FileTarget
        if self._result_location:
            return target(self._result_location)
        return self.run.root_task.task_band

    @result_location.setter
    def result_location(self, path):
        # type: (FileTarget) -> None
        self._result_location = path

    @property
    def settings(self):
        return self.databand_context.settings

    @property
    def config(self):
        return self.databand_context.config


@contextlib.contextmanager
def set_active_run_context(run):
    # type: (DatabandRun) -> Iterator[DatabandRun]

    from dbnd._core.context.databand_context import DatabandContext  # noqa: F811

    with DatabandContext.context(_context=run.context):
        with DatabandRun.context(_context=run) as dr:
            with RunExecutor.context(_context=run.run_executor) as de:
                yield dr


class _RunExecutor_Task(Task):
    """
    Main purpose of this task is to wrap "RunExecutor" execution
    so we can see logs, some important values as task parameters

    RunExecutor -> _Task -> ... (whatever runs here is available as "DriverTaskRun")

    """

    task_is_system = True
    # we don't want child task (root task) to inherit anything from this one
    _conf__scoped_params = False

    def run(self):
        executor_task_run = current_task_run()
        run_executor = executor_task_run.run.run_executor
        run_executor_type = run_executor.run_executor_type
        try:
            if run_executor_type == SystemTaskName.driver:
                return run_executor.run_driver()
            elif run_executor_type == SystemTaskName.driver_submit:
                return run_executor.run_submitter()
            else:
                raise DatabandSystemError(
                    "Unsupported run executor type: %s" % run_executor_type
                )
        except BaseException as ex:
            # we print it on any exception
            logger.warning("Run failure: %s" % ex)
            logger.warning(
                "\n\n\n\n{sep}\n\n   -= Your run has failed, please review errors below =-\n\n{sep}\n".format(
                    sep=console_utils.error_separator()
                )
            )

            failed_msgs = []
            canceled_msgs = []
            for task_run in executor_task_run.run.get_task_runs():
                if task_run.task_run_state == TaskRunState.FAILED:
                    failed_msgs.append(
                        task_run.task.ctrl.banner(
                            msg="Task has failed!", color="red", task_run=task_run
                        )
                    )
                elif task_run.task_run_state == TaskRunState.CANCELLED:
                    canceled_msgs.append(
                        task_run.task.ctrl.banner(
                            msg="Task has been canceled!",
                            color="yellow",
                            task_run=task_run,
                        )
                    )

            if canceled_msgs:
                logger.warning(
                    "\nNumber of canceled tasks={count}:\n{banner}\n".format(
                        banner="\n".join(canceled_msgs), count=len(canceled_msgs)
                    )
                )

            if failed_msgs:
                logger.warning(
                    "\nNumber of failed tasks={count}:\n{banner}\n".format(
                        banner="\n".join(failed_msgs), count=len(failed_msgs)
                    )
                )
            raise


def dbnd_run_task(
    task_or_task_name: Union[Task, str],
    context,
    job_name: Optional[str] = None,
    force_task_name: Optional[str] = None,
    project: Optional[str] = None,
    run_uid: Optional[UUID] = None,
    existing_run: Optional[bool] = None,
    scheduled_run_info: Optional[ScheduledRunInfo] = None,
    send_heartbeat: bool = True,
) -> RunExecutor:
    """
    This is the main entry point to run task in "dbnd orchestration" mode
    called from `dbnd run`
    we create a new Run + RunExecutor and trigger the execution

    :param task_or_task_name task name to run or already built task object
    :param force_task_name
    :param project Project name for the run
    :return RunExecutor
    """
    job_name = get_task_name_safe(job_name or task_or_task_name)
    project_name = get_project_name_safe(
        project
        or context.config.get("tracking", "project")
        or context.settings.tracking.project,
        task_or_task_name,
    )

    with DatabandRun.new_context(
        context=context,
        job_name=job_name,
        run_uid=run_uid,
        existing_run=existing_run,
        scheduled_run_info=scheduled_run_info,
        is_orchestration=True,
        project_name=project_name,
        allow_override=True,
    ) as run:  # type: DatabandRun
        # this is the main entry point to run some task in "orchestration" mode
        with RunExecutor.new_context(
            run=run,
            root_task_or_task_name=task_or_task_name,
            force_task_name=force_task_name,
            send_heartbeat=send_heartbeat,
            allow_override=True,
        ) as run_executor:
            run.run_executor = run_executor
            run_executor.run_execute()
        return run_executor


def _get_dbnd_run_relative_cmd():
    """returns command without 'dbnd run' prefix"""
    argv = list(sys.argv)
    while argv:
        current = argv.pop(0)
        if current == "run":
            return argv
    raise DatabandRunError(
        "Can't calculate run command from '%s'",
        help_msg="Check that it has a format of '..executable.. run ...'",
    )


def _run_user_func(param, value):
    if not value:
        return
    return run_user_func(value)
