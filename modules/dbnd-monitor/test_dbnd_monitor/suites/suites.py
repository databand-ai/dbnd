# © Copyright Databand.ai, an IBM Company 2025

from dbnd_monitor.error_handling.error_aggregator import (
    ComponentErrorAggregator,
    ErrorAggregator,
)
from test_dbnd_monitor.plugins.mock_generic_syncer import WithGenericSyncer


class WhenComponentErrorDoesNotSupported(WithGenericSyncer):
    """
    This class emulates explicitly forbidden Component Error reporting mode
    """

    environment = {"DBND__MONITOR__COMPONENT_ERROR_SUPPORT": "FALSE"}
    config_extension = {"component_error_support": False}
    aggregator = ErrorAggregator


class WhenComponentErrorSupportedByWebServerOnly(WithGenericSyncer):
    """
    Assume that only Web Server is set to support ComponentError,
    monitor by default does not support ComponentError and must be
    explicitly set up in order to report exceptions as ComponentError.
    """

    environment = {}
    config_extension = {"component_error_support": True}
    aggregator = ErrorAggregator


class WhenComponentErrorSupportedByMonitorOnly(WithGenericSyncer):
    """
    Assume that only Monitor is set to support ComponentError. In that
    situation it will report exceptions in old manner, since Web
    Server is not ready to process new format of reporting.
    """

    environment = {"DBND__MONITOR__COMPONENT_ERROR_SUPPORT": "TRUE"}
    config_extension = {}
    aggregator = ErrorAggregator


class WhenComponentErrorSupportedByBothSides(WithGenericSyncer):
    """
    This class emulated the state, when reporting Component Error is desired

    To make ComponentError supported across the system, both Web Server
    and Monitor must be set to support it. On a Web Server side it's
    the feature flag called "component_error_support" that turns on the mode,
    and on monitor's side  the environment variable called
    "DBND__MONITOR__COMPONENT_ERROR_SUPPORT".
    """

    environment = {"DBND__MONITOR__COMPONENT_ERROR_SUPPORT": "TRUE"}
    config_extension = {"component_error_support": True}
    aggregator = ComponentErrorAggregator
