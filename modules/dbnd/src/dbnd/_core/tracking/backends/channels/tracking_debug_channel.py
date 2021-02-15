import logging
import pprint

from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.backends.channels.marshmallow_mixin import MarshmallowMixin


logger = logging.getLogger(__name__)


class ConsoleDebugTrackingChannel(MarshmallowMixin, TrackingChannel):
    """Json API client implementation."""

    def __init__(self, print_func=None):
        super(ConsoleDebugTrackingChannel, self).__init__()
        self.print_func = print_func or logger.info
        self._printer = pprint.PrettyPrinter(indent=2, width=140)

    def _handle(self, name, data):
        self.print_func("tracking %s():\n%s" % (name, self._printer.pformat(data)))

    def is_ready(self):
        return True
