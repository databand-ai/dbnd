from dbnd._core.current import get_databand_context
from dbnd._core.errors.base import (
    DatabandConnectionException,
    TrackerPanicError,
    TrackerRecoverError,
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
                "Failed to connect the tracking api",
                e,
                help_msg="As a temporal workaround you can disable WEB tracking using --disable-web-tracker",
            )
        except TypeError as e:
            # probably problems with data trying to send - can recover
            raise TrackerRecoverError("Failed to send the data", e)
        # unknown exception are not handled

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "Web"
