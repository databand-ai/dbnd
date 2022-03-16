import logging
import typing

from uuid import UUID


if typing.TYPE_CHECKING:
    from typing import List, Optional, Union

    from dbnd._core.constants import (
        DbndDatasetOperationType,
        DbndTargetOperationStatus,
        DbndTargetOperationType,
        UpdateSource,
    )
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd.api.tracking_api import (
        AirflowTaskInfo,
        InitRunArgs,
        LogDatasetArgs,
        LogTargetArgs,
    )
    from targets.base_target import Target
    from targets.value_meta import ValueMeta

logger = logging.getLogger(__name__)


state_call_registry = set()


def state_call(func):
    state_call_registry.add(func.__name__)
    return func


def is_state_call(func_name):
    return func_name in state_call_registry


class TrackingStore(object):
    def __init__(self, *args, **kwargs):
        pass

    @state_call
    def init_scheduled_job(self, scheduled_job, update_existing):
        pass

    @state_call
    def init_run(self, run):
        # type: (DatabandRun) -> List[int]
        pass

    @state_call
    def init_run_from_args(self, init_args):
        # type: (InitRunArgs) -> List[int]
        pass

    @state_call
    def set_run_state(self, run, state, timestamp=None):
        pass

    @state_call
    def set_task_reused(self, task_run):
        pass

    @state_call
    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        pass

    @state_call
    def set_task_run_states(self, task_runs):
        pass

    @state_call
    def set_unfinished_tasks_state(self, run_uid, state):
        pass

    def save_task_run_log(self, task_run, log_body, local_log_path=None):
        pass

    def save_external_links(self, task_run, external_links_dict):
        pass

    def log_histograms(self, task_run, key, value_meta, timestamp):
        pass

    def log_metrics(self, task_run, metrics):
        pass

    def log_artifact(self, task_run, name, artifact, artifact_target):
        pass

    @state_call
    def add_task_runs(self, run, task_runs):
        pass

    @state_call
    def heartbeat(self, run_uid):
        pass

    @state_call
    def update_task_run_attempts(self, task_run_attempt_updates):
        pass

    @state_call
    def save_airflow_task_infos(self, airflow_task_infos, source, base_url):
        # type: (List[AirflowTaskInfo], UpdateSource, str) -> None
        pass

    def log_dataset(
        self,
        task_run,  # type: TaskRun
        operation_path,  # type: Union[Target, str]
        data_meta,  # type: ValueMeta
        operation_type,  # type: DbndDatasetOperationType
        operation_status,  # type: DbndTargetOperationStatus
        operation_error,  # type: Optional[str]
        with_partition=None,  # type: Optional[bool]
    ):  # type: (...) -> None
        pass

    def log_datasets(self, datasets_info):
        # type: (List[LogDatasetArgs]) -> None
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

    def log_dbt_metadata(self, dbt_run_metadata, task_run):
        pass

    def flush(self):
        pass
