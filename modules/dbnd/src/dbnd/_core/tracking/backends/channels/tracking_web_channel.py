from dbnd._core.current import get_databand_context
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.utils.basics.memoized import cached


class TrackingWebChannel(TrackingChannel):
    """Json API client implementation."""

    @property
    @cached()
    def client(self):
        return get_databand_context().databand_api_client

    def _handle(self, name, data):
        # disabled for now, should be changed back on 0.26
        # api_endpoint = "tracking/%s" % name

        api_endpoint = name
        return self.client.api_request(api_endpoint, data)

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "Web"
