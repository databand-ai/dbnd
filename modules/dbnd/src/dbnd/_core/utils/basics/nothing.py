# © Copyright Databand.ai, an IBM Company 2022

from typing import Any


class _Nothing(object):
    """
    based on Attrs version
    Sentinel class to indicate the lack of a value when ``None`` is ambiguous.

    All instances of `_Nothing` are equal.
    """

    def __copy__(self):
        return self

    def __deepcopy__(self, _):
        return self

    def __eq__(self, other):
        return other.__class__ == _Nothing

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "NOTHING"

    def __hash__(self):
        return 0xC0FFEE

    def __bool__(self):
        return False

    def __len__(self):
        return 0

    def __nonzero__(self):
        return False


NOTHING = _Nothing()


def is_defined(value):  # type: (Any) -> bool
    if value is None:
        return True  # value defined as None

    # we can not check with   'value is NOTHING' as NOTHING ref is changed after serialization/deserialization
    if isinstance(value, _Nothing):
        return False

    return True


def is_not_defined(value):
    return not is_defined(value)


def get_or_default(value, default):
    if is_defined(value):
        return value
    return default
