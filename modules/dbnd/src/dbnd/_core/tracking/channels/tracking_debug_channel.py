import logging
import pprint

from dbnd.api.tracking_api import TrackingAPI


logger = logging.getLogger(__name__)


class ConsoleDebugTrackingChannel(TrackingAPI):
    """Json API client implementation."""

    def __init__(self, print_func=None):
        super(ConsoleDebugTrackingChannel, self).__init__()
        self.print_func = print_func or logger.info
        self._printer = pprint.PrettyPrinter(indent=2, width=140)

    def _handle(self, name, data):
        self.print_func("tracking %s():\n%s" % (name, self._printer.pformat(data)))

    def is_ready(self):
        return True


# We can implement file tracking channel the moment we can have proper tracker.close() handling
# class FileTrackingChannel(TrackingAPI):
#     """Json API client implementation."""
#
#     def __init__(self, print_func=None):
#         super(FileTrackingChannel, self).__init__()
#         self.print_func = print_func or logger.info
#         self._printer = pprint.PrettyPrinter(indent=2, width=140)
#
#     def _handle(self, name, data):
#         self.print_func("tracking %s():\n%s" % (name, self._printer.pformat(data)))
