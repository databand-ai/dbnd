import datetime
import functools
import json
import operator

from collections import OrderedDict
from typing import Mapping
from uuid import UUID

import dbnd._vendor.hjson as hjson

from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.traversing import DatabandDict
from dbnd._core.utils.type_check_utils import is_instance_by_class_name


class _FrozenOrderedDict(Mapping, DatabandDict):
    """
    .. deprecated:: 0.7.3

    It is an immutable wrapper around ordered dictionaries that implements the complete :py:class:`collections.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability and ordering are desired.
    """

    def __init__(self, *args, **kwargs):
        self.__dict = OrderedDict(*args, **kwargs)
        self.__hash = None

    def __getitem__(self, key):
        return self.__dict[key]

    def __iter__(self):
        return iter(self.__dict)

    def __len__(self):
        return len(self.__dict)

    def __repr__(self):
        return repr(self.__dict)

    def __hash__(self):
        if self.__hash is None:
            hashes = map(hash, self.items())
            self.__hash = functools.reduce(operator.xor, hashes, 0)

        return self.__hash

    def get_wrapped(self):
        return self.__dict

    def copy(self):
        return _FrozenOrderedDict(self.__dict)


def json_default(obj, safe=False):
    from dbnd._core.parameter.parameter_definition import ParameterDefinition

    if isinstance(obj, ParameterDefinition):
        return str(obj)

    if isinstance(obj, _FrozenOrderedDict):
        return obj.get_wrapped()

    from targets import Target

    if isinstance(obj, Target):
        return str(obj)

    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d")

    if is_instance_by_class_name(obj, "int32") or is_instance_by_class_name(
        obj, "int64"
    ):
        return str(obj)

    if isinstance(obj, UUID):
        return str(obj)
    if safe:
        return str(obj)

    raise TypeError(repr(obj) + " is not JSON serializable")


def dumps(obj, default=json_default, sort_keys=True, **kwargs):
    return json.dumps(obj, default=default, sort_keys=sort_keys, **kwargs)


def loads(s, **kwargs):
    if windows_compatible_mode:
        if s and s[0] in ["{", "["]:
            try:
                return loads_canonical(s, **kwargs)
            except Exception:
                return hjson.loads(s, **kwargs)
        return s
    return hjson.loads(s, **kwargs)


def loads_canonical(s, **kwargs):
    return json.loads(s, **kwargs)


def dumps_canonical(obj, separators=(",", ":"), sort_keys=True, default=json_default):
    return json.dumps(obj, separators=separators, sort_keys=sort_keys, default=default)


def dumps_safe(
    obj,
    separators=(",", ":"),
    sort_keys=True,
    default=functools.partial(json_default, safe=True),
):
    return dumps_canonical(
        obj=obj, separators=separators, sort_keys=sort_keys, default=default
    )


def convert_to_safe_types(obj):
    return loads(dumps_safe(obj))
