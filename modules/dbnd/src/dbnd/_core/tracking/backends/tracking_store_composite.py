import logging

from dbnd._core.errors.base import TrackerPanicError, TrackerRecoverError
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.backends import TrackingStore, TrackingStoreThroughChannel


logger = logging.getLogger(__name__)


class CompositeTrackingStore(TrackingStore):
    def __init__(self, tracking_stores, raise_on_error=True, remove_failed_store=False):
        if not tracking_stores:
            logger.warning("You are running without any tracking store configured.")
        self._stores = tracking_stores
        self._raise_on_error = raise_on_error
        self._remove_failed_store = remove_failed_store

    def _invoke(self, name, kwargs):
        res = None
        failed_stores = []
        for store in self._stores:
            try:
                handler = getattr(store, name)
                handler_res = handler(**kwargs)
                if handler_res:
                    res = handler_res

            except TrackerRecoverError as ex:
                log_exception(
                    "Failed to store tracking information from %s at %s"
                    % (name, str(store)),
                    ex,
                    non_critical=True,
                )

            except TrackerPanicError as ex:
                log_exception(
                    "Failed to store tracking information from %s at %s"
                    % (name, str(store)),
                    ex,
                    non_critical=True,
                )

                if self._remove_failed_store:
                    failed_stores.append(store)
                if self._raise_on_error:
                    raise

            except Exception as ex:
                if self._remove_failed_store:
                    failed_stores.append(store)
                log_exception(
                    "Failed to store tracking information from %s at %s: %s"
                    % (name, str(store), str(ex)),
                    ex,
                    non_critical=True,
                )
        if failed_stores:
            for store in failed_stores:
                logger.warning(
                    "Removing store %s from stores list due to failure" % (str(store),)
                )
                self._stores.remove(store)
        return res

    # this is a function that used for disabling Tracking api on spark inline tasks.
    def disable_tracking_api(self):
        filtered_stores = []

        for store in self._stores:
            if isinstance(store, TrackingStoreThroughChannel):
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

    def log_histograms(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_histograms.__name__, kwargs)

    def log_metrics(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_metrics.__name__, kwargs)

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

    def save_airflow_monitor_data(self, **kwargs):
        return self._invoke(
            CompositeTrackingStore.save_airflow_monitor_data.__name__, kwargs
        )
