import logging
import os
import queue
import threading

from time import sleep
from typing import Any, Dict

import attr

from dbnd._core.current import in_tracking_run, is_orchestration_run
from dbnd._core.errors.base import TrackerPanicError
from dbnd._core.tracking.backends.abstract_tracking_store import is_state_call
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.backends.channels.marshmallow_mixin import MarshmallowMixin
from dbnd._core.tracking.backends.channels.tracking_web_channel import (
    TrackingWebChannel,
)
from dbnd._core.tracking.backends.tracking_store_composite import try_run_handler


logger = logging.getLogger(__name__)

_TERMINATOR = object()


class TrackingAsyncWebChannelBackgroundWorker(object):
    def __init__(self, item_processing_handler, skip_processing_callback):

        self._lock = threading.Lock()

        self.item_processing_handler = item_processing_handler
        self.skip_processing_callback = skip_processing_callback

        self._queue = queue.Queue()
        self._thread = None
        self._thread_for_pid = None

    @property
    def is_alive(self) -> bool:
        if self._thread_for_pid != os.getpid():
            return False
        if not self._thread:
            return False
        return self._thread.is_alive()

    def start(self) -> None:
        with self._lock:
            if not self.is_alive:
                self._thread = threading.Thread(
                    target=self._thread_worker,
                    daemon=True,
                    name="dbnd.TrackingAsyncWebChannelBackgroundWorker",
                )
                self._thread.start()
                self._thread_for_pid = os.getpid()

    def shutdown(self) -> None:
        logger.debug("background worker got shutdown request")
        with self._lock:
            if self._thread:
                self._queue.put(_TERMINATOR)
                self._queue.join()
                self._thread = None
                self._thread_for_pid = None

    def _thread_worker(self) -> None:
        failed_on_previous_iteration = False
        while True:
            item = self._queue.get()
            try:
                if item is _TERMINATOR:
                    break
                if not failed_on_previous_iteration:
                    self.item_processing_handler(item)
                else:
                    self.skip_processing_callback(item)
            except Exception as exc:
                # It's better to continue cleanning the queue with queue.task_done() to avoid hanging on queue.join()
                failed_on_previous_iteration = True
                logger.warning(
                    "TrackingAsyncWebChannelBackgroundWorker will skip processing next events"
                )
            finally:
                self._queue.task_done()
            sleep(0)

    def _ensure_thread(self) -> None:
        if not self.is_alive:
            self.start()

    def submit(self, item: Any) -> None:
        self._ensure_thread()
        self._queue.put(item)


@attr.s
class AsyncWebChannelQueueItem(object):
    name: str = attr.ib()
    data: Dict[str, Any] = attr.ib()
    is_orchestration_run: str = attr.ib()
    stop_tracking_on_failure: str = attr.ib()


class TrackingAsyncWebChannel(MarshmallowMixin, TrackingChannel):
    """Json API client implementation with non-blocking calls."""

    def __init__(
        self,
        max_retries,
        remove_failed_store,
        tracker_raise_on_error,
        is_verbose,
        databand_api_client,
        *args,
        **kwargs,
    ):
        super(TrackingAsyncWebChannel, self).__init__(*args, **kwargs)

        self.web_channel = TrackingWebChannel(databand_api_client)

        self._background_worker = TrackingAsyncWebChannelBackgroundWorker(
            item_processing_handler=self._background_worker_item_handler,
            skip_processing_callback=self._background_worker_skip_processing_callback,
        )

        self._max_retries = max_retries
        self._remove_failed_store = remove_failed_store
        self._tracker_raise_on_error = tracker_raise_on_error
        self._is_verbose = is_verbose
        self._log_fn = logger.info if self._is_verbose else logger.debug
        self._shutting_down = False

    def _handle(self, name, data):
        if self._shutting_down:
            # May happen if the store is used after databand ctx is exited
            raise RuntimeError("TrackingAsyncWebChannel is invoked after shutdown")

        # read tracking args from current runtime to avoid thread from reading a newer runtime context
        stop_tracking_on_failure = self._remove_failed_store or (
            in_tracking_run() and is_state_call(name)
        )

        # send data for processing in a thread
        item = AsyncWebChannelQueueItem(
            name=name,
            data=data,
            is_orchestration_run=is_orchestration_run(),
            stop_tracking_on_failure=stop_tracking_on_failure,
        )
        self._background_worker.submit(item=item)

    def _background_worker_item_handler(self, item: AsyncWebChannelQueueItem):
        try:
            tries = self._max_retries if is_state_call(item.name) else 1
            self._log_fn(
                f"TrackingAsyncWebChannel.thread_worker processing {item.name} "
            )
            try_run_handler(tries, self.web_channel, item.name, {"data": item.data})
            self._log_fn(f"TrackingAsyncWebChannel.thread_worker completed {item.name}")

        except Exception as exc:
            logger.warning(
                "Exception in TrackingAsyncWebChannel in background worker",
                exc_info=True,
            )
            if item.stop_tracking_on_failure:
                raise exc

            if isinstance(exc, TrackerPanicError) and self._tracker_raise_on_error:
                if item.is_orchestration_run:
                    # in orchestration runs we have good error collection that's show error banner
                    # error should have good msg and no need to show full trace
                    exc.show_exc_info = False

                raise exc
            # in all other cases continue
            pass

    def _background_worker_skip_processing_callback(
        self, item: AsyncWebChannelQueueItem
    ):
        self._log_fn(
            f"TrackingAsyncWebChannel skips {item.name} tracking event due to a previous failure"
        )

    def shutdown(self):
        # skip the handler if worker already exited to avoid hanging
        if not self._background_worker.is_alive:
            return
        # process remaining items in the queue
        logger.info("Waiting for TrackingAsyncWebChannel to complete async tasks...")
        self._shutting_down = True
        self._background_worker.shutdown()
        self.web_channel.shutdown()
        logger.info("TrackingAsyncWebChannel completed all tasks")

    def __str__(self):
        return "AsyncWeb"
