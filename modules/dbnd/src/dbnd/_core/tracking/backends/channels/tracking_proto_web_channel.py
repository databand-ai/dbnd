import base64

from dbnd._core.settings.run_info import RunInfoConfig
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.uid_utils import get_uuid
from dbnd.api.proto.generated.tracking_pb2 import (
    Event,
    PostEventsRequest,
    PostEventsResponse,
)


def str_type(obj):
    return "%s.%s" % (obj.__class__.__module__, obj.__class__.__name__)


class TrackingProtoWebChannel(TrackingChannel):
    """Proto API client implementation."""

    def __init__(self, databand_api_client, *args, **kwargs):
        super(TrackingProtoWebChannel, self).__init__(*args, **kwargs)
        self.client = databand_api_client

    @property
    @cached()
    def source_version(self):
        return RunInfoConfig().source_version

    def _handle(self, name, data):
        labels = {
            "sender": "TrackingProtoWebChannel",
            "source_version": self.source_version,
            # TODO: add env details
        }

        event = Event(uuid=str(get_uuid()), schema=name, labels=labels)
        event.data.update(data)

        post_event_request = PostEventsRequest()
        post_event_request.events.append(event)
        post_event_request.timestamp.GetCurrentTime()

        raw_bytes = post_event_request.SerializeToString()
        encoded_str = base64.b64encode(raw_bytes).decode("utf-8")
        data = {"data": encoded_str}

        response = self.client.api_request("tracking/proto", data)
        encoded_str = response.get("result")
        raw_bytes = base64.b64decode(encoded_str.encode("utf-8"))

        post_event_response = PostEventsResponse()
        post_event_response.ParseFromString(raw_bytes)

        if post_event_response.exception:
            raise Exception("Response error: %s" % post_event_response.exception)

        return post_event_response.responses[event.uuid]

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "ProtoWeb"
