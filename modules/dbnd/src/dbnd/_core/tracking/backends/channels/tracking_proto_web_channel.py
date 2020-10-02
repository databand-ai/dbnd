import base64

from dbnd._core.current import get_databand_context
from dbnd._core.settings.run_info import RunInfoConfig
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.backends.channels.protobuf_mixin import ProtobufMixin
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.uid_utils import get_uuid
from dbnd.api.proto.generated.tracking_pb2 import (
    Event,
    PostEventsRequest,
    PostEventsResponse,
)


def str_type(obj):
    return "%s.%s" % (obj.__class__.__module__, obj.__class__.__name__)


class TrackingProtoWebChannel(ProtobufMixin, TrackingChannel):
    """Proto API client implementation."""

    @property
    @cached()
    def client(self):
        return get_databand_context().databand_api_client

    @property
    @cached()
    def source_version(self):
        return RunInfoConfig().source_version

    def _handle(self, name, data):
        schema = self.get_schema_by_handler_name(name)

        labels = {
            "sender": "TrackingProtoWebChannel",
            "source_version": self.source_version,
            # TODO: add env details
        }

        event = Event(uuid=str(get_uuid()), schema=str_type(schema), labels=labels)
        event.data.update(data)

        post_event_request = PostEventsRequest()
        post_event_request.events.append(event)
        post_event_request.timestamp.GetCurrentTime()

        data = post_event_request.SerializeToString()

        encoded_response_str = self.client.api_request("tracking/proto", data)
        b64encoded_response_bytes = encoded_response_str.encode("utf-8")
        raw_bytes = base64.b64decode(b64encoded_response_bytes)

        post_event_response = PostEventsResponse()
        post_event_response.ParseFromString(raw_bytes)

        if post_event_response.exception:
            raise Exception("Response error: %s" % post_event_response.exception)

        return post_event_response.responses[event.uuid]

    def is_ready(self):
        return self.client.is_ready()

    def __str__(self):
        return "ProtoWeb"
