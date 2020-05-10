import contextlib
import logging
import os
import sys
import threading
import typing

from datetime import datetime
from typing import Any, ContextManager, Iterator, List, Optional, Union
from uuid import UUID

import six

from dbnd._core.configuration.environ_config import (
    DBND_PARENT_TASK_RUN_ATTEMPT_UID,
    DBND_PARENT_TASK_RUN_UID,
    DBND_RESUBMIT_RUN,
    DBND_ROOT_RUN_TRACKER_URL,
    DBND_ROOT_RUN_UID,
    DBND_RUN_UID,
    ENV_DBND__USER_PRE_INIT,
)
from dbnd._core.constants import (
    DescribeFormat,
    RunState,
    SystemTaskName,
    TaskExecutorType,
    TaskRunState,
    UpdateSource,
)
from dbnd._core.current import current_task_run
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.errors.base import (
    DatabandFailFastError,
    DatabandRunError,
    DatabandSigTermError,
)
from dbnd._core.parameter.parameter_builder import output, parameter
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.run.describe_run import DescribeRun
from dbnd._core.run.run_tracker import RunTracker
from dbnd._core.run.target_identity_source_map import TargetIdentitySourceMap
from dbnd._core.run.task_runs_builder import TaskRunsBuilder
from dbnd._core.settings import DatabandSettings, EngineConfig, RunConfig
from dbnd._core.task import Task
from dbnd._core.task_build.task_context import current_task, has_current_task
from dbnd._core.task_build.task_registry import (
    build_task_from_config,
    get_task_registry,
)
from dbnd._core.task_executor.factory import (
    calculate_task_executor_type,
    get_task_executor,
)
from dbnd._core.task_executor.heartbeat_sender import start_heartbeat_sender
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.tracking.tracking_info_run import RootRunInfo, ScheduledRunInfo
from dbnd._core.utils import console_utils
from dbnd._core.utils.basics.load_python_module import load_python_callable
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.date_utils import unique_execution_date
from dbnd._core.utils.traversing import flatten
from dbnd._core.utils.uid_utils import get_uuid
from dbnd._vendor.cloudpickle import cloudpickle
from dbnd._vendor.namesgenerator import get_name_for_uid
from targets import FileTarget, Target
from targets.caching import TARGET_CACHE


if typing.TYPE_CHECKING:
    from uuid import UUID


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext

logger = logging.getLogger(__name__)


def _get_dbnd_run_relative_cmd():
    argv = list(sys.argv)
    while argv:
        current = argv.pop(0)
        if current == "run":
            return argv
    raise DatabandRunError(
        "Can't calculate run command from '%s'",
        help_msg="Check that it has a format of '..executable.. run ...'",
    )


# naive implementation of stop event
# we can't save it on Context (non pickable in some cases like running in multithread python)
# if somebody is killing run it's global for the whole process
_is_killed = threading.Event()


class DatabandRun(SingletonContext):
    def __init__(
        self,
        context,
        task_or_task_name,
        run_uid=None,
        scheduled_run_info=None,
        send_heartbeat=True,
        existing_run=None,
        job_name=None,
        source=UpdateSource.dbnd,
        af_context=None,
    ):
        # type:(DatabandContext, Union[Task, str] , Optional[UUID], Optional[ScheduledRunInfo], Optional[bool], Optional[UpdateSource]) -> None
        self.context = context
        s = self.context.settings  # type: DatabandSettings

        if isinstance(task_or_task_name, six.string_types):
            self.root_task_name = task_or_task_name
            self.root_task = None
        elif isinstance(task_or_task_name, Task):
            self.root_task_name = task_or_task_name.task_name
            self.root_task = task_or_task_name
        else:
            raise

        self.job_name = job_name or self.root_task_name

        self.description = s.run.description
        self.is_archived = s.run.is_archived
        self.source = source
        # this was added to allow the scheduler to create the run which will be continued by the actually run command instead of having 2 separate runs
        if not run_uid and DBND_RUN_UID in os.environ:
            # we pop so if this run spawnes subprocesses with their own runs they will be associated using the sub-runs mechanism instead
            # of being fused into this run directly
            run_uid = os.environ.pop(DBND_RUN_UID)
        if run_uid:
            self.run_uid = run_uid
            self.existing_run = True
        else:
            self.run_uid = get_uuid()
            self.existing_run = False

        if existing_run is not None:
            self.existing_run = existing_run

        self.name = s.run.name or get_name_for_uid(self.run_uid)
        # this is so the scheduler can create a run with partial information and then have the subprocess running the actual cmd fill in the details
        self.resubmit_run = (
            DBND_RESUBMIT_RUN in os.environ
            and os.environ.pop(DBND_RESUBMIT_RUN) == "true"
        )

        # AIRFLOW, move into executor
        # dag_id , execution_date and run_id is used by airflow
        self.dag_id = self.root_task_name
        self.execution_date = unique_execution_date()
        run_id = s.run.id
        if not run_id:
            # we need this name, otherwise Airflow will try to manage our local jobs at scheduler
            # ..zombies cleanup and so on
            run_id = "backfill_{0}_{1}".format(
                self.name, self.execution_date.isoformat()
            )
        self.run_id = run_id

        self._template_vars = self._build_template_vars()

        self.is_tracked = True

        self.runtime_errors = []
        self._run_state = None
        self.task_runs = []  # type: List[TaskRun]
        self.task_runs_by_id = {}
        self.task_runs_by_af_id = {}

        self.target_origin = TargetIdentitySourceMap()
        self.describe = DescribeRun(self)
        self.tracker = RunTracker(self, tracking_store=self.context.tracking_store)

        # ALL RUN CONTEXT SPECIFIC thing
        self.root_run_info = RootRunInfo.from_env(current_run=self)
        self.scheduled_run_info = scheduled_run_info or ScheduledRunInfo.from_env(
            self.run_uid
        )

        # now we can add driver task
        self.driver_task_run = None  # type: Optional[TaskRun]
        self.root_task_run = None  # type: Optional[TaskRun]

        self.run_folder_prefix = os.path.join(
            "log",
            self.execution_date.strftime("%Y-%m-%d"),
            "%s_%s_%s"
            % (
                self.execution_date.strftime("%Y-%m-%dT%H%M%S.%f"),
                self.root_task_name,
                self.name,
            ),
        )

        self.run_config = self.context.settings.run  # type: RunConfig
        self.env = env = self.context.env

        self.local_engine = self._get_engine_config(env.local_engine)
        self.remote_engine = self._get_engine_config(
            env.remote_engine or env.local_engine
        )

        self.submit_driver = (
            self.run_config.submit_driver
            if self.run_config.submit_driver is not None
            else env.submit_driver
        )
        self.submit_tasks = (
            self.run_config.submit_tasks
            if self.run_config.submit_tasks is not None
            else env.submit_tasks
        )
        self.task_executor_type, self.parallel = calculate_task_executor_type(
            self.submit_tasks, self.remote_engine, self.context.settings
        )

        self.sends_heartbeat = send_heartbeat
        self.dynamic_af_tasks_count = dict()
        self.af_context = af_context

    def _get_engine_config(self, name):
        # type: ( Union[str, EngineConfig]) -> EngineConfig
        return build_task_from_config(name, EngineConfig)

    @property
    def run_url(self):
        return self.tracker.run_url

    @property
    def task(self):
        return self.root_task

    @property
    def driver_task(self):
        # type: ()->_DbndDriverTask
        return self.driver_task_run.task

    @property
    def driver_dump(self):
        return self.driver_task_run.task.driver_dump

    def _build_template_vars(self):
        # template vars
        ds = self.execution_date.strftime("%Y-%m-%d")
        ts = self.execution_date.isoformat()
        ds_nodash = ds.replace("-", "")
        ts_nodash = ts.replace("-", "").replace(":", "")
        ts_safe = ts.replace(":", "")

        return {
            "run": self,
            "run_ds": ds,
            "run_ts": ts,
            "run_ds_nodash": ds_nodash,
            "run_ts_nodash": ts_nodash,
            "run_ts_safe": ts_safe,
        }

    # TODO: split to get_by_id/by_af_id
    def get_task_run(self, task_id):
        # type: (str) -> TaskRun
        return self.get_task_run_by_id(task_id) or self.get_task_run_by_af_id(task_id)

    def get_task_run_by_id(self, task_id):
        # type: (str) -> TaskRun
        return self.task_runs_by_id.get(task_id)

    def get_task_run_by_af_id(self, task_id):
        # type: (str) -> TaskRun
        return self.task_runs_by_af_id.get(task_id)

    def get_af_task_ids(self, task_ids):
        return [self.get_task_run(task_id).task_af_id for task_id in task_ids]

    def get_task(self, task_id):
        # type: (str) -> Task
        return self.get_task_run(task_id).task

    @property
    def describe_dag(self):
        return self.root_task.ctrl.describe_dag

    def set_run_state(self, state):
        self._run_state = state
        self.tracker.set_run_state(state)

    def run_dynamic_task(self, task, task_engine=None):
        if task_engine is None:
            task_engine = self.current_engine_config
        task_run = self.create_dynamic_task_run(task, task_engine)
        task_run.runner.execute()
        return task_run

    def _build_driver_task(self):
        if self.submit_driver and not self.existing_run:
            logger.info("Submitting job to remote execution")
            task_name = SystemTaskName.driver_submit
            is_submitter = True
            is_driver = False
            host_engine = self.local_engine.clone(require_submit=False)
            target_engine = self.local_engine.clone(require_submit=False)
            task_executor_type = TaskExecutorType.local
        else:
            task_name = SystemTaskName.driver
            is_submitter = not self.existing_run or self.resubmit_run
            is_driver = True
            task_executor_type = self.task_executor_type

            if self.submit_driver:
                # submit drive is true, but we are in existing run:
                # we are after the jump from submit to driver execution (to remote engine)
                host_engine = self.remote_engine.clone(require_submit=False)
            else:
                host_engine = self.local_engine.clone(
                    require_submit=False
                )  # we are running at this engine already

            target_engine = self.remote_engine
            if not self.submit_tasks or task_executor_type == "airflow_kubernetes":
                target_engine = target_engine.clone(require_submit=False)

        dbnd_local_root = host_engine.dbnd_local_root or self.env.dbnd_local_root
        run_folder_prefix = self.run_folder_prefix

        local_driver_root = dbnd_local_root.folder(run_folder_prefix)
        local_driver_log = local_driver_root.partition("%s.log" % task_name)

        remote_driver_root = self.env.dbnd_root.folder(run_folder_prefix)
        driver_dump = remote_driver_root.file("%s.pickle" % task_name)

        driver_task = _DbndDriverTask(
            task_name=task_name,
            task_version=self.run_uid,
            execution_date=self.execution_date,
            is_submitter=is_submitter,
            is_driver=is_driver,
            host_engine=host_engine,
            target_engine=target_engine,
            task_executor_type=task_executor_type,
            local_driver_root=local_driver_root,
            local_driver_log=local_driver_log,
            remote_driver_root=remote_driver_root,
            driver_dump=driver_dump,
            sends_heartbeat=self.sends_heartbeat,
        )

        tr = TaskRun(task=driver_task, run=self, task_engine=driver_task.host_engine)
        self._add_task_run(tr)
        return tr

    def _on_enter(self):
        if self.driver_task_run is None:
            # we are in submit/driver
            self.driver_task_run = self._build_driver_task()
            self.current_engine_config = self.driver_task_run.task.host_engine
            self.tracker.init_run()
        else:
            # we are in task run ( after the jump)
            self.current_engine_config = self.driver_task_run.task.target_engine.clone(
                require_submit=False
            )

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

        self.set_run_state(run_state)
        self.tracker.tracking_store.set_unfinished_tasks_state(
            run_uid=self.run_uid, state=unfinished_task_state
        )

        err_banner_msg = self.describe.get_error_banner()
        logger.error(
            u"\n\n{sep}\n{banner}\n{sep}".format(
                sep=console_utils.ERROR_SEPARATOR, banner=err_banner_msg
            )
        )
        return DatabandRunError(
            "Run has failed: %s" % ex, run=self, nested_exceptions=ex
        )

    def run_driver(self):
        """
        Runs the main driver!
        """
        # with captures_log_into_file_as_task_file(log_file=self.local_driver_log.path):
        try:
            self.driver_task_run.runner.execute()
        except DatabandRunError as ex:
            self._dbnd_run_error(ex)
            raise
        except (Exception, KeyboardInterrupt, SystemExit) as ex:
            raise self._dbnd_run_error(ex)
        finally:
            try:
                self.driver_task.host_engine.cleanup_after_run()
            except Exception:
                logger.exception("Failed to shutdown the current run, continuing")

        return self

    def _get_task_by_id(self, task_id):
        task = self.context.task_instance_cache.get_task_by_id(task_id)
        if task is None:
            raise DatabandRuntimeError(
                "Failed to find task %s in current context" % task_id
            )

        return task

    def save_run(self, target_file=None):
        """
        dumps current run and context to file
        """
        t = target_file or self.driver_dump
        logger.info("Saving current run into %s", t)
        with t.open("wb") as fp:
            cloudpickle.dump(obj=self, file=fp)

    @contextlib.contextmanager
    def run_context(self):
        # type: (DatabandRun) -> Iterator[DatabandRun]
        from dbnd._core.context.databand_context import DatabandContext  # noqa: F811

        with DatabandContext.context(_context=self.context):
            with DatabandRun.context(_context=self) as dr:
                yield dr  # type: DatabandRun

    @classmethod
    def load_run(self, dump_file, disable_tracking_api):
        # type: (FileTarget, bool) -> DatabandRun
        logger.info("Loading dbnd run from %s", dump_file)
        with dump_file.open("rb") as fp:
            databand_run = cloudpickle.load(file=fp)
            if disable_tracking_api:
                databand_run.context.tracking_store.disable_tracking_api()
                logger.info("Tracking has been disabled")
        try:
            if databand_run.context.settings.core.pickle_handler:
                pickle_handler = load_python_callable(
                    databand_run.context.settings.core.pickle_handler
                )
                pickle_handler(databand_run)
        except Exception as e:
            logger.warning(
                "error while trying to handle pickle with custom handler:", e
            )
        return databand_run

    def get_template_vars(self):
        return self._template_vars

    def create_dynamic_task_run(self, task, task_engine, task_af_id=None):
        if task_af_id is None:
            task_name = task.friendly_task_name
            if task_name in self.dynamic_af_tasks_count:
                self.dynamic_af_tasks_count[task_name] += 1
                task_af_id = "{}_{}".format(
                    task_name, self.dynamic_af_tasks_count[task_name]
                )
            else:
                self.dynamic_af_tasks_count[task_name] = 1
                task_af_id = task_name

        tr = TaskRun(
            task=task,
            run=self,
            is_dynamic=True,
            task_engine=task_engine,
            task_af_id=task_af_id,
        )
        self.add_task_runs([tr])
        return tr

    def add_task_runs(self, task_runs):
        # type: (List[TaskRun]) -> None
        for tr in task_runs:
            self._add_task_run(tr)

        self.tracker.add_task_runs(task_runs)

    def _add_task_run(self, task_run):
        self.task_runs.append(task_run)
        self.task_runs_by_id[task_run.task.task_id] = task_run
        self.task_runs_by_af_id[task_run.task_af_id] = task_run

        task_run.task.ctrl.last_task_run = task_run

    def cleanup_after_task_run(self, task):
        # type: (Task) -> None
        rels = task.ctrl.relations
        # potentially, all inputs/outputs targets for current task could be removed
        targets_to_clean = set(flatten([rels.task_inputs, rels.task_outputs]))

        targets_in_use = set()
        # any target which appears in inputs of all not finished tasks shouldn't be removed
        for tr in self.task_runs:
            if tr.task_run_state in TaskRunState.final_states():
                continue
            # remove all still needed inputs from targets_to_clean list
            for target in flatten(tr.task.ctrl.relations.task_inputs):
                targets_in_use.add(target)

        TARGET_CACHE.clear_for_targets(targets_to_clean - targets_in_use)

    def get_context_spawn_env(self):
        env = {}
        if has_current_task():
            current = current_task()
        else:
            current = self.root_task

        if current:
            tr = self.get_task_run_by_id(current.task_id)
            if tr:
                env[DBND_PARENT_TASK_RUN_UID] = str(tr.task_run_uid)
                env[DBND_PARENT_TASK_RUN_ATTEMPT_UID] = str(tr.task_run_attempt_uid)

        env[DBND_ROOT_RUN_UID] = str(self.root_run_info.root_run_uid)
        env[DBND_ROOT_RUN_TRACKER_URL] = self.root_run_info.root_run_url

        if self.context.settings.core.user_code_on_fork:
            env[ENV_DBND__USER_PRE_INIT] = self.context.settings.core.user_code_on_fork
        return env

    def _init_without_run(self):
        self.driver_task_run.task.build_root_task_runs(self)

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
        called from user space
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
            logger.error("Failed to find current task: %s" % ex)
            return

        if not current_task:
            logger.info("No current task.. Killing nothing..")
            return

        try:
            current_task.on_kill()
        except Exception as ex:
            logger.error("Failed to kill current task %s: %s" % (current_task, ex))
            return

    def get_current_dbnd_local_root(self):
        # we should return here the proper engine config, based in which context we run right now
        # it could be submit, driver or task engine
        return self.env.dbnd_local_root


class _DbndDriverTask(Task):
    _conf__no_child_params = True
    task_is_system = True
    task_in_memory_outputs = True

    is_driver = parameter[bool]
    is_submitter = parameter[bool]
    execution_date = parameter[datetime]
    sends_heartbeat = parameter[bool]

    host_engine = parameter[EngineConfig]
    target_engine = parameter[EngineConfig]

    task_executor_type = parameter[str]

    # all paths, we make them system, we don't want to check if they are exists
    local_driver_root = output(system=True)[Target]
    local_driver_log = output(system=True)[Target]

    remote_driver_root = output(system=True)[Target]
    driver_dump = output(system=True)[Target]

    def _build_submit_task(self, run):
        if run.root_task:
            raise DatabandRuntimeError(
                "Can't send to remote execution task created via code, only command line is supported"
            )

        # dont' describe in local run, do it in remote run
        settings = self.settings
        settings.system.describe = False

        cmd_line_args = (
            ["run"] + _get_dbnd_run_relative_cmd() + ["--run-driver", str(run.run_uid)]
        )

        args = run.remote_engine.dbnd_executable + cmd_line_args

        root_task = run.remote_engine.submit_to_engine_task(
            env=run.env,
            args=args,
            task_name="dbnd_driver_run",
            interactive=settings.run.interactive,
        )
        root_task._conf_confirm_on_kill_msg = (
            "Ctrl-C Do you want to kill your submitted pipeline?"
            "If selection is 'no', this process will detach from the run."
        )
        return root_task

    def _build_root_task(self, run):
        # type: (DatabandRun) -> Task
        if self.is_submitter and not self.is_driver:
            return self._build_submit_task(run)
        else:
            if run.root_task:
                # user has created DatabandRun with existing task
                self.task_meta.add_child(run.root_task.task_id)
                return run.root_task

            logger.info("Building main task '%s'", run.root_task_name)
            root_task = get_task_registry().build_dbnd_task(run.root_task_name)
            logger.info(
                "Task %s has been created (%s children)",
                root_task.task_id,
                len(root_task.ctrl.task_dag.subdag_tasks()),
            )
            return root_task

    def is_save_run(self, run, task_runs):

        core_settings = run.context.settings.core
        if core_settings.always_save_pipeline:
            return True
        if core_settings.disable_save_pipeline:
            return False

        if any(tr.task._conf__require_run_dump_file for tr in task_runs):
            return True

        if self.target_engine.require_submit:
            return True

        if self.task_executor_type == TaskExecutorType.local:
            return False

        if is_airflow_enabled():
            from dbnd_airflow.executors import AirflowTaskExecutorType

            return self.task_executor_type not in [
                AirflowTaskExecutorType.airflow_inprocess,
                TaskExecutorType.local,
            ]
        return True

    def build_task_from_cmd_line(self, task_name):
        return

    def build_root_task_runs(self, run):
        """
        called by .run and inline
        :return:
        """
        run.root_task = self._build_root_task(run)

        # for validation only
        run.root_task.task_dag.topological_sort()

        task_runs = TaskRunsBuilder().build_task_runs(
            run, run.root_task, remote_engine=self.target_engine
        )
        # we need it before to mark root task
        run.add_task_runs(task_runs)
        # for faster access
        run.root_task_run = run.get_task_run(run.root_task.task_id)
        return task_runs

    def run(self):
        driver_task_run = current_task_run()
        run = driver_task_run.run  # type: DatabandRun
        if self.is_submitter:
            run.set_run_state(RunState.RUNNING)

        ctx = run.context
        ctx.settings.git.validate_git_policy()

        # let prepare for remote execution
        run.remote_engine.prepare_for_run(run)

        task_runs = self.build_root_task_runs(run)

        hearbeat = None

        # right now we run describe in local controller only, but we should do that for more
        if self.is_driver:
            if run.context.settings.system.describe:
                run.describe_dag.describe_dag()
                logger.info(run.describe.run_banner("Described!", color="blue"))
                return

            root_task_run = run.root_task_run
            run.root_task.ctrl.banner(
                "Main task '%s' has been created!" % root_task_run.task_af_id,
                color="cyan",
                task_run=root_task_run,
            )
            from dbnd._core.task_ctrl.task_dag_describe import DescribeDagCtrl

            completed = {tr.task.task_id: tr.is_reused for tr in task_runs}
            run_describe_dag = DescribeDagCtrl(
                root_task_run.task, DescribeFormat.short, complete_status=completed
            )
            run_describe_dag.tree_view(describe_format=DescribeFormat.short)

            if self.is_save_run(run, task_runs):
                run.save_run()

            if self.sends_heartbeat:
                hearbeat = start_heartbeat_sender(driver_task_run)

        # create executor without driver task!
        task_executor = get_task_executor(
            run,
            task_executor_type=self.task_executor_type,
            host_engine=self.host_engine,
            target_engine=run.root_task_run.task_engine,
            task_runs=task_runs,
        )

        with nested(hearbeat):
            task_executor.do_run()

        if self.is_driver:
            # This is great success!
            run.set_run_state(RunState.SUCCESS)
            logger.info(run.describe.run_banner_for_finished())
            return run
        else:
            logger.info(run.describe.run_banner_for_submitted())


def new_databand_run(context, task_or_task_name, run_uid=None, **kwargs):
    # type: (DatabandContext, Union[Task, str], UUID, **Any)-> ContextManager[DatabandRun]

    kwargs["allow_override"] = kwargs.pop("allow_override", True)
    return DatabandRun.new_context(
        context=context, task_or_task_name=task_or_task_name, run_uid=run_uid, **kwargs
    )
