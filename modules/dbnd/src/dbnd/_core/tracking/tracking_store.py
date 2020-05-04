import logging
import typing

from uuid import UUID

from dbnd._core.errors.base import DatabandConnectionException


if typing.TYPE_CHECKING:
    from typing import List

    from targets.base_target import Target
    from targets.value_meta import ValueMeta

    from dbnd.api.tracking_api import InitRunArgs, AirflowTaskInfo, LogTargetArgs
    from dbnd._core.constants import (
        DbndTargetOperationType,
        DbndTargetOperationStatus,
        UpdateSource,
    )
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


class TrackingStore(object):
    @staticmethod
    def _serialize_dbnd_objects(dbnd_run):
        # type: (DatabandRun) -> InitRunArgs
        from dbnd._core.tracking.tracking_info_convertor import TrackingInfoBuilder

        init_run_args = TrackingInfoBuilder(dbnd_run).build_init_args()
        return init_run_args

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

    def log_metric(self, task_run, metric, source=None):
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
        target,  # type: Target
        target_meta,  # type: ValueMeta
        operation_type,  # type: DbndTargetOperationType
        operation_status,  # type: DbndTargetOperationStatus
        param_name,  # type: str
        task_def_uid,  # type: UUID
    ):  # type: (...) -> None
        pass

    def log_targets(self, targets_info):
        # type: (List[LogTargetArgs]) -> None
        pass

    def is_ready(self):
        # type: () -> bool
        pass


class CompositeTrackingStore(TrackingStore):
    def __init__(self, stores, raise_on_error=True):
        self._stores = stores
        self._raise_on_error = raise_on_error

    def _invoke(self, name, kwargs):
        res = None
        for store in self._stores:
            try:
                handler = getattr(store, name)
                handler_res = handler(**kwargs)
                if handler_res:
                    res = handler_res
            except DatabandConnectionException as ex:
                logger.error(
                    "Failed to store tracking information from %s at %s : %s"
                    % (name, store.__class__.__name__, ex)
                )
                if self._raise_on_error:
                    raise
            except Exception as ex:
                logger.exception(
                    "Failed to store tracking information from %s at %s"
                    % (name, store.__class__.__name__)
                )
                if self._raise_on_error:
                    raise
        return res

    # this is a function that used for disabling Tracking api on spark inline tasks.
    def disable_tracking_api(self):
        filtered_stores = []
        from dbnd._core.tracking.tracking_store_api import TrackingStoreApi

        for store in self._stores:
            if isinstance(store, TrackingStoreApi):
                continue
            filtered_stores.append(store)
        self._stores = filtered_stores

    def init_scheduled_job(self, **kwargs):
        return self._invoke(CompositeTrackingStore.init_scheduled_job.__name__, kwargs)

    def init_run(self, **kwargs):
        return self._invoke(CompositeTrackingStore.init_run.__name__, kwargs)

    def init_run_from_args(self, **kwargs):
        return self._invoke(CompositeTrackingStore.init_run_from_args.__name__, kwargs)

    def set_run_state(self, **kwargs):
        return self._invoke(CompositeTrackingStore.set_run_state.__name__, kwargs)

    def set_task_reused(self, **kwargs):
        return self._invoke(CompositeTrackingStore.set_task_reused.__name__, kwargs)

    def set_task_run_state(self, **kwargs):
        return self._invoke(CompositeTrackingStore.set_task_run_state.__name__, kwargs)

    def set_task_run_states(self, **kwargs):
        return self._invoke(CompositeTrackingStore.set_task_run_states.__name__, kwargs)

    def set_unfinished_tasks_state(self, **kwargs):
        return self._invoke(
            CompositeTrackingStore.set_unfinished_tasks_state.__name__, kwargs
        )

    def save_task_run_log(self, **kwargs):
        return self._invoke(CompositeTrackingStore.save_task_run_log.__name__, kwargs)

    def save_external_links(self, **kwargs):
        return self._invoke(CompositeTrackingStore.save_external_links.__name__, kwargs)

    def log_target(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_target.__name__, kwargs)

    def log_targets(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_targets.__name__, kwargs)

    def log_metric(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_metric.__name__, kwargs)

    def log_artifact(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_artifact.__name__, kwargs)

    def close(self):
        pass

    def add_task_runs(self, **kwargs):
        return self._invoke(CompositeTrackingStore.add_task_runs.__name__, kwargs)

    def heartbeat(self, **kwargs):
        return self._invoke(CompositeTrackingStore.heartbeat.__name__, kwargs)

    def save_airflow_task_infos(self, **kwargs):
        return self._invoke(
            CompositeTrackingStore.save_airflow_task_infos.__name__, kwargs
        )

    def update_task_run_attempts(self, **kwargs):
        return self._invoke(
            CompositeTrackingStore.update_task_run_attempts.__name__, kwargs
        )

    def is_ready(self, **kwargs):
        return all(store.is_ready() for store in self._stores)
