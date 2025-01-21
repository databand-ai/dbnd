# © Copyright Databand.ai, an IBM Company 2024

from datetime import datetime
from traceback import format_exception
from typing import List, Optional

import attr

from dbnd._core.utils.timezone import utcnow


def auxiliary_converter(_, attrib: attr.Attribute, value):
    if attrib.name == "timestamp" and isinstance(value, datetime):
        return value.isoformat()
    return value


@attr.s(hash=False, str=False)
class ComponentError:
    exception_type: str = attr.ib()
    exception_body: str = attr.ib()
    traceback: str = attr.ib()
    timestamp: datetime = attr.ib()

    def __hash__(self) -> int:
        return hash(self.traceback)

    def __str__(self):
        timestamp = self.timestamp.isoformat()
        return (
            f"[{timestamp}] {self.exception_type}: {self.exception_body}\n"
            f"<<<\n{self.traceback}>>>"
        )

    @classmethod
    def from_exception(cls, exc: Exception):
        if not isinstance(exc, Exception):
            raise ValueError("Expected an exception instance")

        tb = "".join(format_exception(type(exc), exc, exc.__traceback__))
        return cls(
            exception_type=type(exc).__name__,
            exception_body=str(exc),
            traceback=tb,
            timestamp=utcnow(),
        )

    def dump(self):
        return attr.asdict(self, value_serializer=auxiliary_converter)


@attr.s
class ReportErrorsDTO:
    tracking_source_uid: str = attr.ib()
    external_id: Optional[str] = attr.ib()
    component: str = attr.ib()
    errors: List["ComponentError"] = attr.ib(factory=list)
    is_error: bool = attr.ib(default=True)

    def dump(self):
        return attr.asdict(self, recurse=True, value_serializer=auxiliary_converter)
