# © Copyright Databand.ai, an IBM Company 2022

from typing import Any


def get_callable_name(object: Any) -> str:
    # functools.partial does not has attribute __name__, so we need use repr
    return getattr(object, "__name__", repr(object))
