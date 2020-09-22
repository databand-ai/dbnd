"""
Tracking store backends registry
"""

import logging
import typing

from dbnd._core.errors import friendly_error
from dbnd._core.plugin.dbnd_plugins import assert_web_enabled
from dbnd._core.tracking.backends import (
    CompositeTrackingStore,
    ConsoleStore,
    FileTrackingStore,
    TbSummaryFileStore,
    TrackingStore,
    TrackingStoreThroughChannel,
)


logger = logging.getLogger(__name__)


_BACKENDS_REGISTRY = {
    "file": FileTrackingStore,
    "console": ConsoleStore,
    "debug": TrackingStoreThroughChannel.build_with_console_debug_channel,
    ("api", "web"): TrackingStoreThroughChannel.build_with_web_channel,
    ("api", "proto"): TrackingStoreThroughChannel.build_with_proto_web_channel,
    ("api", "disabled"): TrackingStoreThroughChannel.build_with_disabled_channel,
}


def register_store(name, store_builder):
    if name in _BACKENDS_REGISTRY:
        raise Exception("Already registered")
    _BACKENDS_REGISTRY[name] = store_builder


def get_tracking_store(
    tracking_store_names, api_channel_name, tracker_raise_on_error, remove_failed_store
):
    # type: (...) -> TrackingStore
    tracking_store_instances = []
    for name in tracking_store_names:
        if name == "api":
            name = (name, api_channel_name)
        if name == ("api", "db"):
            assert_web_enabled(
                "It is required when trying to use local db connection (tracker_api=db)."
            )
        tracking_store_builder = _BACKENDS_REGISTRY.get(name)
        if tracking_store_builder is None:
            friendly_error.config.wrong_store_name(name)
        instance = tracking_store_builder()
        tracking_store_instances.append(instance)

    # if tracker_raise_on_error is False - must use CompositeTrackingStore
    # regardless of tracking_stores - only CompositeTrackingStore suppresses errors
    return (
        tracking_store_instances[0]
        if len(tracking_store_instances) == 1 and tracker_raise_on_error
        else CompositeTrackingStore(
            tracking_stores=tracking_store_instances,
            raise_on_error=tracker_raise_on_error,
            remove_failed_store=remove_failed_store,
        )
    )
