from collections import Mapping

import pandas as pd
import six


class DatabandDict(object):
    pass


def flatten(struct):
    """
    Creates a flat list of all all items in structured output (dicts, lists, items):

    .. code-block:: python

        >>> sorted(flatten({'a': 'foo', 'b': 'bar'}))
        ['bar', 'foo']
        >>> sorted(flatten(['foo', ['bar', 'troll']]))
        ['bar', 'foo', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, (dict, Mapping)):
        for _, result in six.iteritems(struct):
            flat += flatten(result)
        return flat
    if isinstance(struct, six.string_types):
        return [struct]

    if getattr(struct, "target_no_traverse", None):
        return [struct]

    try:
        # if iterable
        iterator = iter(struct)
    except TypeError:
        return [struct]

    for result in iterator:
        flat += flatten(result)
    return flat


def f_identity(x):
    return x


def f_not_none(x):
    return x


def f_no_filter(x):
    return True


def traverse(
    struct,
    convert_f=f_identity,
    filter_none=False,
    filter_empty=False,
    convert_types=None,
):
    """
    Maps all Tasks in a structured data object to their .output().
    :param convert_types non basic types to apply convert_f
    :param filter_none  remove None from structures
    """

    def t(obj):
        if convert_types and isinstance(obj, convert_types):
            return convert_f(obj)

        if isinstance(obj, Mapping):
            # noinspection PyArgumentList
            converted = ((k, t(v)) for k, v in six.iteritems(obj))
            if filter_none:
                converted = ((k, v) for k, v in converted if v is not None)
            new_obj = obj.__class__(converted)
            if filter_empty and not new_obj:
                return None
            return new_obj

        if isinstance(obj, six.string_types):
            return convert_f(obj)

        if "pyspark" in str(type(obj)):
            return convert_f(obj)

        try:
            target_no_traverse = hasattr(obj, "target_no_traverse") and getattr(
                obj, "target_no_traverse", None
            )
        except ValueError:
            # SPARK OBJECTS do not support hasattr
            target_no_traverse = True
        if target_no_traverse is bool and target_no_traverse:
            return convert_f(obj)

        list_obj_constructor = None
        if isinstance(obj, (list, tuple, set)):
            list_obj_constructor = obj.__class__
        elif isinstance(obj, pd.DataFrame):
            pass
        else:
            try:
                iter(obj)  # noqa: F841
                list_obj_constructor = list
            except TypeError:
                pass

        # we can parse and reconstruct list object
        if list_obj_constructor:
            converted = (t(r) for r in obj)
            if filter_none:
                converted = filter(lambda x: x is not None, converted)
            new_obj = list_obj_constructor(converted)
            if filter_empty and not new_obj:
                return None
            return new_obj

        # so it's simple obj, let apply function
        return convert_f(obj)

    return t(struct)


def _frozen_set(struct):
    if isinstance(struct, set):
        from dbnd._core.utils import json_utils

        return sorted(struct, key=lambda x: json_utils.dumps_canonical(x))
    return struct


def getpaths(struct):
    """
    Maps all Tasks in a structured data object to their .output().
    """
    return traverse(struct, lambda t: t.task_outputs)


def traverse_to_str(obj):
    if obj is None:
        return None
    return traverse(obj, convert_f=str)


def traverse_frozen_set(obj):
    return traverse(obj, convert_types=(set,), convert_f=_frozen_set)
