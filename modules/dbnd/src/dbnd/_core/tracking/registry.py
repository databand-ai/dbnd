# © Copyright Databand.ai, an IBM Company 2022

"""
Tracking store backends registry
"""

import logging
import typing

from typing import List, Optional

from dbnd._core.errors import friendly_error
from dbnd._core.log import dbnd_log_debug
from dbnd._core.tracking.backends import (
    CompositeTrackingStore,
    ConsoleStore,
    TrackingStoreThroughChannel,
)
from dbnd._core.tracking.backends.tracking_store_composite import build_store_name


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext

logger = logging.getLogger(__name__)

_BACKENDS_REGISTRY = {
    "console": ConsoleStore,
    "debug": TrackingStoreThroughChannel.build_with_console_debug_channel,
    ("api", "web"): TrackingStoreThroughChannel.build_with_web_channel,
    ("api", "async-web"): TrackingStoreThroughChannel.build_with_async_web_channel,
    ("api", "disabled"): TrackingStoreThroughChannel.build_with_disabled_channel,
}


def register_store(name, store_builder):
    _BACKENDS_REGISTRY[name] = store_builder


def get_tracking_store(
    databand_ctx: "DatabandContext",
    tracking_store_names: List[str],
    api_channel_name: Optional[str],
    max_retires: int,
    tracker_raise_on_error: bool,
    remove_failed_store: bool,
) -> CompositeTrackingStore:
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

    dbnd_log_debug("Creating tracking stores: %s " % tracking_store_names)
    for name in tracking_store_names:
        if name == "api":
            name = (name, api_channel_name)

        tracking_store_builder = _BACKENDS_REGISTRY.get(name)
        if tracking_store_builder is None:
            raise friendly_error.config.wrong_store_name(
                name, [str(k) for k in _BACKENDS_REGISTRY.keys()]
            )

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
