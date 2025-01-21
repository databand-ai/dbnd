# © Copyright Databand.ai, an IBM Company 2022


from operator import attrgetter
from typing import Optional, Set

import attr

from dbnd._core.utils.timezone import utcnow
from dbnd_monitor.error_handling.component_error import ComponentError


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


class ComponentErrorAggregator:
    def __init__(self):
        self.active_errors: Set[ComponentError] = set()
        self.errors: Set[ComponentError] = set()

    def report_component_error(self, component_error: ComponentError):
        self.active_errors.add(component_error)

    def report(self):
        # type: () -> tuple[list[ComponentError], bool]
        """
        Returns a tuple of list containing ComponentError instances reported
        during the cycle and a boolean indicating whether the list must be sent
        to the DB. In case `active_errors` hashes is the same as `errors` ones
        there is nothing to report, so the returning list is empty.
        """

        def hashed(comp_errors: set[ComponentError]) -> set[int]:
            return {hash(comp_error) for comp_error in comp_errors}

        if hashed(self.errors) != hashed(self.active_errors):
            self.errors = self.active_errors.copy()
            self.active_errors.clear()
            return (
                sorted(self.errors, key=attrgetter("timestamp"), reverse=True),
                True,
            )

        self.active_errors.clear()
        return [], False
