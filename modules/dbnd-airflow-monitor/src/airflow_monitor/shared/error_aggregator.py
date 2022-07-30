# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from weakref import WeakKeyDictionary

import attr

from dbnd._core.utils.timezone import utcnow


@attr.s
class ErrorAggregatorResult:
    message = attr.ib()  # type: Optional[str]
    should_update = attr.ib()  # type: bool


class ErrorAggregator:
    def __init__(self):
        # we use WeakKeyDictionary and not regular dict() since keys used for
        # reporting can be objects, and we want to make sure to clean up those
        # keys/messages when then no longer exist.
        # the main use case is that passed key in function object, in theory it can be
        # destroyed, in which case it will be stuck here forever (=means reporting to
        # webserver forever). Practically currently it's not happening since decorators
        # are on class functions and not on specific objects, but this is just to be
        # safe. A little bit more real world example would be:
        # class A:
        #   def something(self):
        #     try:
        #       raise NotImplementedError()
        #     except Exception as e:
        #       report_error(self.something, str(e))
        #
        # for i in range(10):
        #   A().something()
        # in this case it will be all 10 different "keys"
        self.active_errors = WeakKeyDictionary()
        self.last_reported_errors = 0

    def report(self, key, message: Optional[str]) -> ErrorAggregatorResult:
        if message is not None:
            self.active_errors[key] = (utcnow(), message)
        elif key in self.active_errors:
            del self.active_errors[key]
        elif self.last_reported_errors == len(self.active_errors):
            # we get here if
            #  * message is None (current invocation was ok)
            #  * key not in self.active_errors (previous invocation was ok)
            #  * # of errors didn't change (no weakref evictions)
            # => so nothing to update
            return ErrorAggregatorResult(None, should_update=False)

        self.last_reported_errors = len(self.active_errors)

        sorted_message = [
            msg for _, msg in sorted(self.active_errors.values(), reverse=True)
        ]
        aggregated_error = "\n\n---------\n\n".join(sorted_message)
        # return None if empty string
        return ErrorAggregatorResult(aggregated_error or None, should_update=True)
