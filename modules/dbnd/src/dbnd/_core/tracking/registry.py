# © Copyright Databand.ai, an IBM Company 2022

"""
Tracking store backends registry
"""

import logging

from typing import List, Optional

from dbnd._core.errors import friendly_error
from dbnd._core.tracking.backends import (
    CompositeTrackingStore,
    ConsoleStore,
    FileTrackingStore,
    TrackingStoreThroughChannel,
)
from dbnd._core.tracking.backends.tracking_store_composite import build_store_name


logger = logging.getLogger(__name__)

_BACKENDS_REGISTRY = {
    "file": FileTrackingStore,
    "console": ConsoleStore,
    "debug": TrackingStoreThroughChannel.build_with_console_debug_channel,
    ("api", "web"): TrackingStoreThroughChannel.build_with_web_channel,
    ("api", "async-web"): TrackingStoreThroughChannel.build_with_async_web_channel,
    ("api", "disabled"): TrackingStoreThroughChannel.build_with_disabled_channel,
}


def register_store(name, store_builder):
    if name in _BACKENDS_REGISTRY:
        raise Exception("Already registered")
    _BACKENDS_REGISTRY[name] = store_builder


def get_tracking_store(
    databand_ctx,
    tracking_store_names,
    api_channel_name,
    max_retires,
    tracker_raise_on_error,
    remove_failed_store,
):
    # type: (List[str], Optional[str], int, bool, bool) -> CompositeTrackingStore
    """
    Build a tracking stores based on the registry and wrap them with TrackingStoreComposite

    @param tracking_store_names: names of the wanted tracking stores
    @param api_channel_name: specify the name of the api channel if required
    @param max_retires: the amounts of retries to allowed the tracking store to run requests
    @param tracker_raise_on_error: True if the tracking store should raise fatal errors, False otherwise.
    @param remove_failed_store: True if a failed tracking store should be removed, False otherwise.
    @return:
    """

    tracking_store_instances = {}
    for name in tracking_store_names:
        if name == "api":
            name = (name, api_channel_name)

        tracking_store_builder = _BACKENDS_REGISTRY.get(name)
        if tracking_store_builder is None:
            raise friendly_error.config.wrong_store_name(name)

        instance = tracking_store_builder(databand_ctx)
        if isinstance(name, tuple):
            store_name = build_store_name(*name)
        else:
            store_name = name

        tracking_store_instances[store_name] = instance

    # We always use the CompositeTrackingStore cause it handle failure recovery for the inner tracking stores
    return CompositeTrackingStore(
        tracking_stores=tracking_store_instances,
        max_retires=max_retires,
        raise_on_error=tracker_raise_on_error,
        remove_failed_store=remove_failed_store,
    )
