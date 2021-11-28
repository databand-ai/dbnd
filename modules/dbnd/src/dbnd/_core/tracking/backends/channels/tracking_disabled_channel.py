import logging

from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.backends.channels.marshmallow_mixin import MarshmallowMixin


logger = logging.getLogger(__name__)


class DisabledTrackingChannel(MarshmallowMixin, TrackingChannel):
    def __init__(self):
        super(DisabledTrackingChannel, self).__init__()
        logger.info("Tracking store is disable at core.tracker_api.")

    def is_ready(self):
        return True

    def _handle(self, name, data):
        pass
