import logging
import typing

from uuid import UUID


if typing.TYPE_CHECKING:
    from typing import List, Optional, Union

    from targets.base_target import Target
    from targets.value_meta import ValueMeta

    from dbnd.api.tracking_api import (
        InitRunArgs,
        AirflowTaskInfo,
        LogTargetArgs,
    )
    from dbnd._core.constants import (
        DbndTargetOperationType,
        DbndTargetOperationStatus,
        UpdateSource,
    )
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


class TrackingStore(object):
    def init_scheduled_job(self, scheduled_job, update_existing):
        pass

    def init_run(self, run):
        # type: (DatabandRun) -> List[int]
        pass

    def init_run_from_args(self, init_args):
        # type: (InitRunArgs) -> List[int]
        pass

    def set_run_state(self, run, state, timestamp=None):
        pass

    def set_task_reused(self, task_run):
        pass

    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        pass

    def set_task_run_states(self, task_runs):
        pass

    def set_unfinished_tasks_state(self, run_uid, state):
        pass

    def save_task_run_log(self, task_run, log_body):
        pass

    def save_external_links(self, task_run, external_links_dict):
        pass

    def log_histograms(self, task_run, key, histogram_spec, value_meta, timestamp):
        pass

    def log_metrics(self, task_run, metrics):
        pass

    def log_artifact(self, task_run, name, artifact, artifact_target):
        pass

    def add_task_runs(self, run, task_runs):
        pass

    def heartbeat(self, run_uid):
        pass

    def update_task_run_attempts(self, task_run_attempt_updates):
        pass

    def save_airflow_task_infos(self, airflow_task_infos, source, base_url):
        # type: (List[AirflowTaskInfo], UpdateSource, str) -> None
        pass

    def log_target(
        self,
        task_run,  # type: TaskRun
        target,  # type: Union[Target, str]
        target_meta,  # type: ValueMeta
        operation_type,  # type: DbndTargetOperationType
        operation_status,  # type: DbndTargetOperationStatus
        param_name=None,  # type: Optional[str]
        task_def_uid=None,  # type: Optional[UUID]
    ):  # type: (...) -> None
        pass

    def log_targets(self, targets_info):
        # type: (List[LogTargetArgs]) -> None
        pass

    def is_ready(self):
        # type: () -> bool
        pass
