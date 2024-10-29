# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional

import attr

from dbnd._core.utils.timezone import utcnow


@attr.s
class ErrorAggregatorResult:
    message = attr.ib()  # type: Optional[str]
    should_update = attr.ib()  # type: bool


class ErrorAggregator:
    """
    The purpose of this class is that if one syncer reports an error and another does not,
    we would still have the error message reported and not cleaned it up.
    The key must be str and not any other object.
    """

    def __init__(self):
        self.active_errors = {}

    def report(self, key: str, message: Optional[str]) -> ErrorAggregatorResult:
        if message is not None:
            self.active_errors[key] = (utcnow(), message)
        elif key in self.active_errors:
            del self.active_errors[key]
        else:
            return ErrorAggregatorResult(None, should_update=False)

        sorted_message = [
            msg for _, msg in sorted(self.active_errors.values(), reverse=True)
        ]
        aggregated_error = "\n\n---------\n\n".join(sorted_message)
        # return None if empty string
        return ErrorAggregatorResult(aggregated_error or None, should_update=True)
