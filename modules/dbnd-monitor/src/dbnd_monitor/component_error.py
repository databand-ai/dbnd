# © Copyright Databand.ai, an IBM Company 2024

from datetime import datetime
from traceback import format_exception

from pydantic import BaseModel


class ComponentError(BaseModel):
    exception_type: str  # Exception type
    exception_message: str  # Exception message
    exception_traceback: str  # Stack traceback
    timestamp: datetime  # UTC datetime

    def __hash__(self) -> int:
        return hash(self.exception_traceback)

    def __str__(self):
        timestamp = datetime.strftime(self.timestamp, "%Y-%m-%d %H:%M:%S")
        return (
            f"[{timestamp}] {self.exception_type}: {self.exception_message}\n"
            f"<<<\n{self.exception_traceback}>>>"
        )

    @classmethod
    def from_exception(cls, exc: Exception):
        if not isinstance(exc, Exception):
            raise ValueError("Expected an exception instance")

        tb = "".join(format_exception(type(exc), exc, exc.__traceback__))
        return cls(
            exception_type=type(exc).__name__,
            exception_message=str(exc),
            exception_traceback=tb,
            timestamp=datetime.now(),
        )
