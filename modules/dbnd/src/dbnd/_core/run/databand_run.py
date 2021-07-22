import logging
import os
import typing

from typing import Any, ContextManager, List, Optional, Union
from uuid import UUID

from dbnd._core.configuration.environ_config import (
    DBND_PARENT_TASK_RUN_ATTEMPT_UID,
    DBND_PARENT_TASK_RUN_UID,
    DBND_ROOT_RUN_TRACKER_URL,
    DBND_ROOT_RUN_UID,
    DBND_RUN_UID,
    ENV_DBND__USER_PRE_INIT,
)
from dbnd._core.constants import (
    AD_HOC_DAG_PREFIX,
    RESULT_PARAM,
    SystemTaskName,
    TaskRunState,
    UpdateSource,
)
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.run.run_banner import RunBanner
from dbnd._core.run.run_tracker import RunTracker
from dbnd._core.run.target_identity_source_map import TargetIdentitySourceMap
from dbnd._core.settings import DatabandSettings
from dbnd._core.settings.engine import build_engine_config
from dbnd._core.task import Task
from dbnd._core.task_build.task_context import current_task, has_current_task
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.tracking.schemas.tracking_info_run import RootRunInfo, ScheduledRunInfo
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.date_utils import unique_execution_date
from dbnd._core.utils.traversing import flatten
from dbnd._core.utils.uid_utils import get_uuid
from dbnd._vendor.namesgenerator import get_random_name
from targets.caching import TARGET_CACHE


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.task_executor.run_executor import RunExecutor

logger = logging.getLogger(__name__)


class DatabandRun(SingletonContext):
    """
    Main object that represents Pipeline/Tasks/Flow Run
    it controls
     * the overall state of the execution
     * tracking of the whole run
     * tracking of the data objects between different tasks
    Main two modes:
     * tracking - driver is optional, root task has to be assigned
     * orchestration - driver will be assigned by RunExecutor
    """

    def __init__(
        self,
        context,  # type: DatabandContext
        job_name,
        project_name=None,  # type: Optional[str]
        run_uid=None,  # type:  Optional[UUID]
        scheduled_run_info=None,  # type:  Optional[ScheduledRunInfo]
        existing_run=None,
        source=UpdateSource.dbnd,  # type:Optional[UpdateSource]
        af_context=None,
        tracking_source=None,
        is_orchestration=False,
    ):
        self.context = context
        s = self.context.settings  # type: DatabandSettings

        self.job_name = job_name
        self.project_name = project_name or s.tracking.project

        self.description = s.run.description
        self.is_archived = s.run.is_archived
        self.source = source
        self.is_orchestration = is_orchestration

        self.existing_run = existing_run or False
        # this was added to allow the scheduler to create the run which will be continued by the actually run command instead of having 2 separate runs
        if not run_uid and DBND_RUN_UID in os.environ:
            # we pop so if this run spawnes subprocesses with their own runs they will be associated using the sub-runs mechanism instead
            # of being fused into this run directly
            run_uid = UUID(os.environ.pop(DBND_RUN_UID))
        if run_uid:
            self.run_uid = run_uid
            self.existing_run = True
        else:
            self.run_uid = get_uuid()

        # if user provided name - use it
        # otherwise - generate human friendly name for the run
        self.name = s.run.name or get_random_name(seed=self.run_uid)
        self.execution_date = (
            self.context.settings.run.execution_date or unique_execution_date()
        )

        self.is_tracked = True

        # tracking/orchestration main task
        self.root_task = None  # type: Optional[Task]

        # task run that wraps execution (tracking or orchestration)
        self._driver_task_run = None

        # ORCHESTRATION: execution of the run
        self.run_executor = None  # type: Optional[RunExecutor]

        # dag_id , execution_date are used by Airflow,
        # should be deprecated (still used by DB tracking)
        self.dag_id = (
            (AD_HOC_DAG_PREFIX + self.job_name) if is_orchestration else self.job_name
        )

        # RUN STATE
        self._run_state = None
        self.task_runs = []  # type: List[TaskRun]
        self.task_runs_by_id = {}
        self.task_runs_by_af_id = {}

        self.target_origin = TargetIdentitySourceMap()
        self.describe = RunBanner(self)
        self.tracker = RunTracker(self, tracking_store=self.context.tracking_store)

        # ALL RUN CONTEXT SPECIFIC thing
        self.root_run_info = RootRunInfo.from_env(current_run=self)
        self.scheduled_run_info = scheduled_run_info or ScheduledRunInfo.from_env(
            self.run_uid
        )
        self.env = self.context.env
        self.run_folder_prefix = os.path.join(
            "log",
            self.execution_date.strftime("%Y-%m-%d"),
            "%s_%s_%s"
            % (
                self.execution_date.strftime("%Y-%m-%dT%H%M%S.%f"),
                self.job_name,
                self.name,
            ),
        )
        self.run_root = self.env.dbnd_root.folder(self.run_folder_prefix)
        self.run_local_root = self.env.dbnd_local_root.folder(self.run_folder_prefix)

        self.local_engine = build_engine_config(self.env.local_engine).clone(
            require_submit=False
        )

        self.dynamic_af_tasks_count = dict()
        self.af_context = af_context
        self.tracking_source = tracking_source
        self.start_time = None
        self.finished_time = None
        self._result_location = None

    def get_task_runs(self, without_executor=True, without_system=False):
        """
        :param without_executor: filter driver/submitter task runs
        :param without_system:   filter task.is_system tasks
        :return: List[TaskRun]
        """
        task_runs = self.task_runs
        if without_executor:
            task_runs = [
                tr
                for tr in task_runs
                if tr.task.task_name not in SystemTaskName.driver_and_submitter
            ]
        if without_system:
            task_runs = [tr for tr in task_runs if not tr.task.task_is_system]
        return task_runs

    @property
    def root_task_run(self):  # type: (...)->  Optional[TaskRun]
        if self.root_task:
            return self.get_task_run_by_id(self.root_task.task_id)
        return None

    @property
    def driver_task_run(self):
        """
        This is the main "wrapper" task that will have the "log" of the code execution
        :return:
        """
        # in case orchestration/luigi/others has explicit task run
        if self._driver_task_run:
            return self._driver_task_run
        # otherwise we should use just a main task as a "driver" task (tracking case)
        return self.root_task_run

    @driver_task_run.setter
    def driver_task_run(self, value):
        self._driver_task_run = value

    def build_and_set_driver_task_run(self, driver_task, driver_engine=None):
        """
        set driver task run which is used to "track" main execution flow
        Tracking: will track pipeline progress
        Orchestration: will track and "execute" pipeline

        you need to run DatabandRun.init_run otherwise it will be not tracked
        """
        self._driver_task_run = TaskRun(
            task=driver_task, run=self, task_engine=driver_engine or self.local_engine
        )
        self._add_task_run(self._driver_task_run)
        return self._driver_task_run

    @property
    def run_url(self):
        return self.tracker.run_url

    @property
    def task(self):
        return self.root_task

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

    @property
    def duration(self):
        if self.finished_time and self.start_time:
            return self.finished_time - self.start_time
        return None

    def _get_task_by_id(self, task_id):
        task = self.context.task_instance_cache.get_task_by_id(task_id)
        if task is None:
            raise DatabandRuntimeError(
                "Failed to find task %s in current context" % task_id
            )

        return task

    def next_af_task_name(self, task):
        """
        user friendly task name:
            task_HASH -> task
            task_HASH2 -> task_1
        :param task:
        :return:
        """
        task_name = task.friendly_task_name
        if task_name in self.dynamic_af_tasks_count:
            self.dynamic_af_tasks_count[task_name] += 1
            task_af_id = "{}_{}".format(
                task_name, self.dynamic_af_tasks_count[task_name]
            )
        else:
            self.dynamic_af_tasks_count[task_name] = 1
            task_af_id = task_name
        return task_af_id

    def create_task_run_at_execution_time(self, task, task_engine, task_af_id=None):
        if task_af_id is None:
            task_af_id = self.next_af_task_name(task)

        if self.af_context:
            task_af_id = "_".join([self.af_context.task_id, task_af_id])

        tr = TaskRun(
            task=task,
            run=self,
            is_dynamic=True,
            task_engine=task_engine,
            task_af_id=task_af_id,
        )
        self.add_task_runs_and_track([tr])
        return tr

    def add_task_runs_and_track(self, task_runs):
        # type: (List[TaskRun]) -> None
        for tr in task_runs:
            self._add_task_run(tr)

        self.tracker.add_task_runs(task_runs)

    def _build_and_add_task_run(
        self, task, task_engine=None, task_af_id=None, try_number=1
    ):
        if task_af_id is None:
            task_af_id = self.next_af_task_name(task)

        tr = TaskRun(
            task=task,
            run=self,
            task_engine=task_engine or self.local_engine,
            task_af_id=task_af_id,
            try_number=try_number,
        )
        self._add_task_run(tr)
        return tr

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

    def is_killed(self):
        if self.run_executor:
            return self.run_executor.is_killed()
        return False

    def kill(self):
        """
        called from user space, kills the current task only
        :return:
        """
        if self.run_executor:
            self.run_executor.kill()

    def kill_run(self, message=None):
        if self.run_executor:
            self.run_executor.kill_run(message)

    def get_current_dbnd_local_root(self):
        # we should return here the proper engine config, based in which context we run right now
        # it could be submit, driver or task engine
        return self.env.dbnd_local_root

    def load_from_result(self, name=RESULT_PARAM, value_type=None):
        # type: (Optional[str], Optional[str]) -> Any
        """
        Loads the result of the run by name, default is the default result name.
        """
        return self.run_executor.result.load(name, value_type)


def new_databand_run(context, job_name, run_uid=None, **kwargs):
    # type: (DatabandContext, Union[Task, str], UUID, **Any)-> ContextManager[DatabandRun]

    kwargs["allow_override"] = kwargs.pop("allow_override", True)
    return DatabandRun.new_context(
        context=context, job_name=job_name, run_uid=run_uid, **kwargs
    )
