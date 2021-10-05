import sys
import typing


class PathStr(str):
    pass


if sys.version_info > (3, 5):  # pragma: nocover
    # noinspection PyUnresolvedReferences
    import pathlib

    # noinspection PyUnresolvedReferences
    from pathlib import Path
else:  # pragma: nocover
    # noinspection PyUnresolvedReferences
    import pathlib2 as pathlib

    # noinspection PyUnresolvedReferences
    from pathlib2 import Path


class NullableStr(str):
    pass


# Data typing
class LazyLoad(object):
    pass


class EagerLoad(object):
    pass


T = typing.TypeVar("T")
KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


class DataList(typing.List[T], LazyLoad):
    pass


class DataDict(typing.Dict[KT, VT], LazyLoad):
    pass
