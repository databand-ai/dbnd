from dbnd._core.current import get_databand_context
from dbnd._core.errors.base import (
    DatabandAuthenticationError,
    DatabandConnectionException,
    DatabandUnauthorizedApiError,
    TrackerPanicError,
)
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.backends.channels.marshmallow_mixin import MarshmallowMixin
from dbnd._core.utils.basics.memoized import cached


class TrackingWebChannel(MarshmallowMixin, TrackingChannel):
    """Json API client implementation."""

    @property
    @cached()
    def client(self):
        return get_databand_context().databand_api_client

    def _handle(self, name, data):
        try:
            return self.client.api_request("tracking/%s" % name, data)

        except DatabandConnectionException as e:
            # connection problems are not recoverable for web tracker
            raise TrackerPanicError(
                "Failed to connect the tracking api", inner_error=e,
            ) from None

        except (DatabandAuthenticationError, DatabandUnauthorizedApiError) as e:
            # authentication problems are not recoverable for web tracker
            raise TrackerPanicError(
                "Authentication error accrued", inner_error=e
            ) from None

        # unknown exception are not handled

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "Web"
