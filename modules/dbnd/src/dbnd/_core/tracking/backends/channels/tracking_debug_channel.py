# © Copyright Databand.ai, an IBM Company 2022

import logging
import pprint

from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel


logger = logging.getLogger(__name__)


class ConsoleDebugTrackingChannel(TrackingChannel):
    """Json API client implementation."""

    def __init__(self, print_func=None, *args, **kwargs):
        super(ConsoleDebugTrackingChannel, self).__init__(*args, **kwargs)
        self.print_func = print_func or logger.info
        self._printer = pprint.PrettyPrinter(indent=2, width=140)

    def _handle(self, name, data):
        self.print_func("tracking %s():\n%s" % (name, self._printer.pformat(data)))

    def is_ready(self):
        return True
