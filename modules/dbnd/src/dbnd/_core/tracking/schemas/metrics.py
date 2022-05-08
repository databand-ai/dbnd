import json
import logging

from decimal import Decimal

import attr
import six

from dbnd._core.constants import _DbndDataClass
from dbnd._core.utils import json_utils
from dbnd._core.utils.string_utils import safe_short_string


logger = logging.getLogger(__name__)


def safe_value_to_str(value, max_len):
    if value is None or isinstance(value, six.string_types):
        return safe_short_string(str(value), max_len)

    try:
        # local import to prevent imports recursion
        from targets.values import get_value_type_of_obj

        value_type = get_value_type_of_obj(value)

        if value_type is not None:
            return value_type.to_preview(value, max_len)
    except Exception:
        logger.warning("failed dumping metric, falling back to str", exc_info=True)
        # This object has to hold only serializable values
    return safe_short_string(str(value), max_len)


class Metric(_DbndDataClass):
    _int_precision = 16
    # Workaround for precission issues with large ints
    MAX_INT = 10 ** (_int_precision - 1)
    MIN_INT = -MAX_INT

    VALUE_MAX_LEN = 5000

    def __init__(
        self,
        key,
        timestamp,
        value_int=None,
        value_float=None,
        value_str=None,
        value_json=None,
        value=None,
        source=None,
    ):
        self.key = key
        self.timestamp = timestamp
        self.source = source
        self.value_int = value_int
        self.value_float = value_float
        self.value_str = value_str
        self.value_json = None
        if value_json is not None:
            try:
                self.value_json = json.loads(json_utils.dumps_safe(value_json))
                self.value_str = None
            except Exception:
                self.value_json = None
                self.value_str = safe_value_to_str(value_json, self.VALUE_MAX_LEN)
        elif (value_int, value_float, value_str) == (None, None, None):
            self.value = value

    @property
    def value(self):
        if self.value_float is not None:
            return self.value_float
        if self.value_int is not None:
            return int(self.value_int)
        if self.value_json is not None:
            return self.value_json
        return self.value_str

    @property
    def serialized_value(self):
        if self.value_json is not None:
            return json.dumps(self.value_json, sort_keys=True)
        else:
            return str(self.value)

    @value.setter
    def value(self, value):
        if isinstance(value, Decimal):
            self.value_float = float(value)
        elif isinstance(value, float):
            self.value_float = value
        elif isinstance(value, int) and self.MIN_INT <= value <= self.MAX_INT:
            # need int() in case it's bool
            self.value_int = int(value)
        else:
            # if passed value has json-serializable representation, we'd prefer
            # to store it as value_json. For this we'll try to convert value to
            # preview (to support max length). If resulting object is valid json
            # then we'll store it. If json is not valid or we have any other
            # error, we'll fallback to just converting it to string
            value_str = safe_value_to_str(value, self.VALUE_MAX_LEN)

            try:
                self.value_json = json.loads(value_str)
            except Exception:
                self.value_str = value_str

    def __repr__(self):
        return "Metric(key={}, source={})".format(self.key, self.source)


@attr.s
class Artifact(_DbndDataClass):
    path = attr.ib()  # type: str
