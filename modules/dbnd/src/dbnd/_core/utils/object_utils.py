import datetime
import time

import pendulum
import six

from tabulate import tabulate


def apply_patch(base, delta, fields_filter=lambda f: not f.startswith("_")):
    delta_dict = delta if isinstance(delta, dict) else vars(delta)

    for k, v in delta_dict.items():
        if fields_filter(k) and v is not None:
            base.__setattr__(k, v)


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
    """ backward support for timestamp in py2"""
    if six.PY3:
        return value.timestamp()

    return time.mktime(value.timetuple())
