from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel


class TrackingWebChannel(TrackingChannel):
    """Json API client implementation."""

    def __init__(self, databand_api_client, *args, **kwargs):
        super(TrackingWebChannel, self).__init__(*args, **kwargs)
        self.client = databand_api_client

    def _handle(self, name, data):
        # this method may raise an  exception  in case of connectivity/server errors
        # such exceptions will be handled on upper layer
        return self.client.api_request("tracking/%s" % name, data)

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "Web"
