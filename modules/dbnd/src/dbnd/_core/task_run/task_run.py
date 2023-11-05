# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from dbnd._core.constants import TaskRunState
from dbnd._core.context.use_dbnd_run import is_dbnd_run_package_installed
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_run.task_run_logging import TaskRunLogManager
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.tracking.registry import get_tracking_store
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.seven import contextlib
from dbnd._core.utils.string_utils import clean_job_name
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_task_run_attempt_uid_by_task_run, get_uuid
from dbnd.providers.spark.spark_jvm_context import jvm_context_manager


logger = logging.getLogger(__name__)
if typing.TYPE_CHECKING:
    from typing import Any

    from dbnd._core.run.databand_run import DatabandRun

    if is_dbnd_run_package_installed():
        from dbnd_run.task.task import Task
        from dbnd_run.task_ctrl.task_run_executor import TaskRunExecutor


class TaskRun(object):
    def __init__(
        self,
        task: typing.Union[TrackingTask, "Task"],
        run: "DatabandRun",
        task_af_id: str = None,
        try_number: int = 1,
        is_dynamic: typing.Optional[bool] = None,
    ):
        # actually this is used as Task uid

        self.task: typing.Union[TrackingTask, Task] = task
        self.run: DatabandRun = run
        self.is_dynamic = is_dynamic if is_dynamic is not None else task.task_is_dynamic
        self.is_system = task.task_is_system
        self.task_af_id = task_af_id or self.task.task_id

        if task.ctrl.force_task_run_uid:
            self.task_run_uid = tr_uid = task.ctrl.force_task_run_uid
            if isinstance(tr_uid, TaskRunUidGen):
                self.task_run_uid = tr_uid.generate_task_run_uid(
                    run=run, task=task, task_af_id=self.task_af_id
                )
        else:
            self.task_run_uid = get_uuid()

        # used by all kind of submission controllers
        self.job_name = clean_job_name(self.task_af_id).lower()
        self.job_id = self.job_name + "_" + str(self.task_run_uid)[:8]

        self.attempt_number = try_number

        self.task_run_attempt_uid = get_task_run_attempt_uid_by_task_run(self)

        self.is_tracked = task._conf__tracked

        if self.is_tracked and self.run.is_tracked:
            tracking_store = self.run.context.tracking_store
        else:
            tracking_store = get_tracking_store(
                self.run.context,
                tracking_store_names=["console"],
                api_channel_name=None,
                max_retires=1,
                tracker_raise_on_error=False,
                remove_failed_store=True,
            )

        self.tracking_store = tracking_store
        self.tracker = TaskRunTracker(task_run=self, tracking_store=tracking_store)

        self.task_tracker_url = self.tracker.task_run_url()
        self.external_resource_urls = dict()
        self.errors = []

        self.is_root = False
        self.is_reused = False
        self.is_skipped = False
        # Task can be skipped as it's not required by any other task scheduled to run
        self.is_skipped_as_not_required = False

        self.airflow_context = None
        self._task_run_state = None

        self.start_time = None
        self.finished_time = None

        self.tracking_log_manager = TaskRunLogManager(task_run=self)

        # orchestration support
        self.task_run_executor: "TaskRunExecutor" = None

    def __getstate__(self):
        d = self.__dict__.copy()
        if "airflow_context" in d:
            # airflow context contains "auto generated code" that can not be pickled (Vars class)
            # we don't need to pickle it as we pickle DAGs separately
            d = self.__dict__.copy()
            del d["airflow_context"]
        return d

    @property
    def task_run_env(self):
        return self.run.context.task_run_env

    @property
    def last_error(self):
        return self.errors[-1] if self.errors else None

    @property
    def task_run_state(self):
        return self._task_run_state

    @task_run_state.setter
    def task_run_state(self, value):
        raise AttributeError("Please use explicit .set_task_run_state()")

    def set_task_run_state(self, state, track=True, error=None):
        # type: (TaskRunState, bool, Any) -> bool
        # Optional bool track param - will send tracker.set_task_run_state() by default
        if not state or self._task_run_state == state:
            return False

        if error:
            self.errors.append(error)
        if state == TaskRunState.RUNNING:
            self.start_time = utcnow()

        self._task_run_state = state
        if track:
            self.tracking_store.set_task_run_state(
                task_run=self, state=state, error=error
            )
        return True

    def set_task_reused(self):
        self._task_run_state = TaskRunState.SUCCESS
        self.tracking_store.set_task_reused(task_run=self)

    def set_external_resource_urls(self, links_dict):
        # if value is None skip
        if links_dict is None:
            return

        links_dict = {link: value for link, value in links_dict.items() if value}

        self.external_resource_urls.update(links_dict)
        self.tracking_store.save_external_links(
            task_run=self, external_links_dict=links_dict
        )

    def set_task_run_attempt(self, attempt_number):
        self.attempt_number = attempt_number
        self.task_run_attempt_uid = get_task_run_attempt_uid_by_task_run(self)

    @contextlib.contextmanager
    def task_run_track_execute(self, capture_log=True):
        current_task = self
        ctx_managers = [self.task.ctrl.task_context(phase=TaskContextPhase.RUN)]

        if capture_log:
            ctx_managers.append(self.tracking_log_manager.capture_task_log())

        ctx_managers.append(jvm_context_manager(current_task))

        with nested(*ctx_managers):
            yield

    def __repr__(self):
        return "TaskRun(id=%s, af_id=%s)" % (self.task.task_id, self.task_af_id)


class TaskRunUidGen(object):
    def generate_task_run_uid(self, run, task, task_af_id):
        return get_uuid()
