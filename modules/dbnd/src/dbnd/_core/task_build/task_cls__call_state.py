import logging
import typing

import attr


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    pass


@attr.s
class TaskCallState(object):
    started = attr.ib(default=False)
    finished = attr.ib(default=False)
    result_saved = attr.ib(default=False)

    result = attr.ib(default=None)

    should_store_result = attr.ib(default=False)

    def start(self):
        self.started = True
        self.finished = False
        self.result = None

    def finish(self, result=None):
        self.finished = True
        if self.should_store_result:
            self.result_saved = True
            self.result = result
