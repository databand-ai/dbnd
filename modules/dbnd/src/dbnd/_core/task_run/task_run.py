import logging
import typing

from dbnd._core.constants import TaskRunState
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.task_run.task_run_logging import TaskRunLogManager
from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.task_run.task_run_runner import TaskRunRunner
from dbnd._core.task_run.task_run_sync_local import TaskRunLocalSyncer
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.task_run.task_sync_ctrl import TaskSyncCtrl
from dbnd._core.tracking.registry import get_tracking_store
from dbnd._core.utils.string_utils import clean_job_name
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_task_run_attempt_uid_by_task_run, get_uuid
from targets import target


logger = logging.getLogger(__name__)
if typing.TYPE_CHECKING:
    from typing import Any
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task.task import Task
    from dbnd._core.settings import EngineConfig


class TaskRun(object):
    def __init__(
        self,
        task,
        run,
        task_af_id=None,
        try_number=1,
        is_dynamic=None,
        task_engine=None,
    ):
        # type: (Task, DatabandRun, str, int, bool, EngineConfig)-> None
        # actually this is used as Task uid

        self.task = task  # type: Task
        self.run = run  # type: DatabandRun
        self.task_engine = task_engine
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

        # custom per task engine , or just use one from global env
        dbnd_local_root = (
            self.task_engine.dbnd_local_root or self.run.env.dbnd_local_root
        )
        self.local_task_run_root = (
            dbnd_local_root.folder(run.run_folder_prefix)
            .folder("tasks")
            .folder(self.task.task_id)
        )

        self.attempt_number = try_number
        self.task_run_attempt_uid = None
        self.attempt_folder = None
        self.meta_files = None
        self.log = None
        self.init_new_task_run_attempt()

        # TODO: inherit from parent task if disabled
        self.is_tracked = task._conf__tracked

        if self.is_tracked and self.run.is_tracked:
            tracking_store = self.run.context.tracking_store
        else:
            tracking_store = get_tracking_store(
                tracking_store_names=["console"],
                api_channel_name=None,
                max_retires=1,
                tracker_raise_on_error=False,
                remove_failed_store=True,
            )

        self.tracking_store = tracking_store
        self.tracker = TaskRunTracker(task_run=self, tracking_store=tracking_store)
        self.runner = TaskRunRunner(task_run=self)
        self.deploy = TaskSyncCtrl(task_run=self)
        self.sync_local = TaskRunLocalSyncer(task_run=self)
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

    def task_run_attempt_file(self, *path):
        return target(self.attempt_folder, *path)

    @property
    def last_error(self):
        return self.errors[-1] if self.errors else None

    def _get_log_files(self):

        log_local = None
        if self.log.local_log_file:
            log_local = self.log.local_log_file.path

        log_remote = None
        if self.log.remote_log_file:
            log_remote = self.log.remote_log_file.path

        return log_local, log_remote

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
            # TODO: Throw exception?
            return

        for link in links_dict:
            if not links_dict[link]:
                # Dict has empty fields
                # TODO: Throw exception?
                return

        self.external_resource_urls.update(links_dict)
        self.tracking_store.save_external_links(
            task_run=self, external_links_dict=links_dict
        )

    def update_task_run_attempt(self, attempt_number):
        if attempt_number is None:
            raise DatabandRuntimeError("cannot set None as the attempt number")

        if self.attempt_number != attempt_number:
            self.attempt_number = attempt_number
            self.init_new_task_run_attempt()

    def init_new_task_run_attempt(self):
        # trying to find if we should use attempt_uid that been set from external process.
        # if so - the attempt_uid is uniquely for this task_run_attempt, and that why we pop.
        self.task_run_attempt_uid = get_task_run_attempt_uid_by_task_run(self)

        self.attempt_folder = self.task._meta_output.folder(
            "attempt_%s_%s" % (self.attempt_number, self.task_run_attempt_uid),
            extension=None,
        )
        self.attempt_folder_local = self.local_task_run_root.folder(
            "attempt_%s_%s" % (self.attempt_number, self.task_run_attempt_uid),
            extension=None,
        )
        self.attemp_folder_local_cache = self.attempt_folder_local.folder("cache")
        self.meta_files = TaskRunMetaFiles(self.attempt_folder)
        self.log = TaskRunLogManager(task_run=self)

    def __repr__(self):
        return "TaskRun(id=%s, af_id=%s)" % (self.task.task_id, self.task_af_id)


class TaskRunUidGen(object):
    def generate_task_run_uid(self, run, task, task_af_id):
        return get_uuid()
