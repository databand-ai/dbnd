# © Copyright Databand.ai, an IBM Company 2022

import datetime
import time

import six

from dbnd._vendor import pendulum
from dbnd._vendor.tabulate import tabulate


def patch_module_attr(module, name, value):
    original_name = "_original_" + name
    if getattr(module, original_name, None):
        return
    setattr(module, original_name, getattr(module, name))
    setattr(module, name, value)


def unpatch_module_attr(module, name, value):
    original_name = "_original_" + name
    if hasattr(module, original_name):
        setattr(module, name, getattr(module, original_name))


def patch_models(patches):
    for module, name, value in patches:
        patch_module_attr(module, name, value)


def unpatch_models(patches):
    for module, name, value in patches:
        unpatch_module_attr(module, name, value)


def tabulate_objects(
    objects,
    fields_filter=lambda f: not f.startswith("_"),
    headers=None,
    to_local_time=True,
):
    if not headers:
        headers = [k for k in dir(objects[0]) if fields_filter(k)]

    values = []
    for o in objects:
        o_values = []
        values.append(o_values)
        for h in headers:
            value = o.__getattribute__(h)
            if to_local_time and value and isinstance(value, datetime.datetime):
                value = pendulum.from_timestamp(
                    _timestamp(value), "local"
                ).to_rfc822_string()
            o_values.append(value)

    return tabulate(values, headers=headers)


def _timestamp(value):
    # type: (datetime.datetime)->float
    """backward support for timestamp in py2"""
    if six.PY3:
        return value.timestamp()

    return time.mktime(value.timetuple())


def safe_isinstance(obj, cls_str):
    """
    Checks whether a given obj implements the class who's name is in the cls_str parameter.
    This means that the isinstance check is performed without looking at the object's type hierarchy, but rather
    by class name.
    """
    if cls_str and obj:
        return cls_str in str(type(obj))
    return False
