# © Copyright Databand.ai, an IBM Company 2024

from datetime import datetime
from traceback import format_exception
from typing import Optional

from pydantic import BaseModel

from dbnd._core.utils.timezone import utcnow


class ComponentError(BaseModel):
    exception_type: str  # Exception type
    exception_body: str  # Exception message
    traceback: str  # Stack traceback
    timestamp: datetime  # UTC datetime

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


class ReportErrorsDTO(BaseModel):
    tracking_source_uid: str
    external_id: Optional[str]
    component: str
    errors: list[ComponentError] = []
    is_error: bool = True
