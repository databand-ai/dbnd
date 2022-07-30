# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Any, Dict

import six

from dbnd._core.current import in_tracking_run, is_orchestration_run
from dbnd._core.errors.base import (
    DatabandAuthenticationError,
    DatabandWebserverNotReachableError,
)
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.backends import TrackingStore, TrackingStoreThroughChannel
from dbnd._core.tracking.backends.abstract_tracking_store import is_state_call


MAX_RETRIES = 2

logger = logging.getLogger(__name__)


def try_run_handler(tries, store, handler_name, kwargs):
    # type: (int, TrackingStore, str, Dict[str, Any]) -> Any
    """
    Locate the handler function to run and will try to run it multiple times.
    If fails all the times -> raise the last error.

    @param tries: maximum amount of retries, positive integer.
    @param store: the store to run its handler
    @param handler_name: the name of the handler to run
    @param kwargs: the input for the handler
    @return: The result of the handler if succeeded, otherwise raise the last error
    """
    try_num = 1
    handler = getattr(store, handler_name)

    while True:
        try:
            return handler(**kwargs)

        except DatabandAuthenticationError as ex:
            # No need to retry with the same credentials, log exception and exit immediately.
            log_exception(
                f"Auth Failed storing tracking information from {handler_name} at {str(store)}",
                ex,
                non_critical=True,
            )
            # Failed auth is also not a reson to stop attempting to track subsequent request.
            # We have an option to process requests with failed auth on demand.
            # So, fake success:
            return {}
        except Exception as ex:
            log_exception(
                f"Try {try_num}/{tries}: Tracking failed from {handler_name} at {str(store)}",
                ex,
                non_critical=True,
            )

            if try_num == tries:
                # raise on the last try
                raise

            try_num += 1


def build_store_name(name, channel_name):
    if channel_name:
        return "{}-{}".format(name, channel_name)

    return name


class CompositeTrackingStore(TrackingStore):
    def __init__(
        self,
        tracking_stores,
        max_retires,
        raise_on_error=True,
        remove_failed_store=False,
    ):
        # type: (Dict[str, TrackingStore], int, bool, bool) -> CompositeTrackingStore

        if not tracking_stores:
            logger.warning("You are running without any tracking store configured.")

        self._stores = tracking_stores
        self._raise_on_error = raise_on_error
        self._remove_failed_store = remove_failed_store
        self._max_retries = max_retires

    def _invoke(self, name, kwargs):
        res = None
        failed_stores = []

        for store_name, store in six.iteritems(self._stores):
            tries = self._max_retries if is_state_call(name) else 1

            try:
                res = try_run_handler(tries, store, name, kwargs)
            except Exception as e:
                if self._remove_failed_store or (
                    in_tracking_run() and is_state_call(name)
                ):
                    failed_stores.append(store_name)

                if isinstance(e, DatabandWebserverNotReachableError):
                    if in_tracking_run():
                        logger.warning(str(e))

                    if is_orchestration_run():
                        # in orchestration runs we have good error collection that's show error banner
                        # error should have good msg and no need to show full trace
                        e.show_exc_info = False

                    if self._raise_on_error:
                        raise e

        if failed_stores:
            for store_name in failed_stores:
                logger.warning(
                    "Removing store {store_name}: {store} from stores list due to failure".format(
                        store_name=store_name, store=str(self._stores.get(store_name))
                    )
                )
                store = self._stores.pop(store_name)
                try:
                    store.flush()
                except Exception:
                    logger.exception(
                        f"Error during flush of {store_name} tracking backend"
                    )

            if not self._stores:
                logger.warning("You are running without any tracking store configured.")

        return res

    # this is a function that used for disabling Tracking api on spark inline tasks.
    def disable_tracking_api(self):
        filtered_stores = []

        for store in self._stores:
            if isinstance(store, TrackingStoreThroughChannel):
                continue
            filtered_stores.append(store)
        self._stores = filtered_stores

    def has_tracking_store(self, name, channel_name=None):
        name = build_store_name(name, channel_name)
        return name in self._stores

    @property
    def trackers_names(self):
        return list(self._stores.keys())

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

    def log_dataset(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_dataset.__name__, kwargs)

    def log_datasets(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_datasets.__name__, kwargs)

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

    def log_dbt_metadata(self, **kwargs):
        return self._invoke(CompositeTrackingStore.log_dbt_metadata.__name__, kwargs)

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
        return all(store.is_ready() for store in self._stores.values())

    def flush(self):
        failed = False

        for name, store in self._stores.items():
            try:
                store.flush()
            except Exception as exc:
                failed = exc
                logger.exception(f"Error during flush of {name} tracking backend")

        if failed and self._raise_on_error:
            raise failed
