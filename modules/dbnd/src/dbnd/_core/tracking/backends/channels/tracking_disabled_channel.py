import logging
import pprint

from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel


logger = logging.getLogger(__name__)


class DisabledTrackingChannel(TrackingChannel):
    def __init__(self, print_func=None):
        super(DisabledTrackingChannel, self).__init__()
        logger.info("Tracking store is disable at core.tracker_api.")

    def is_ready(self):
        return True
