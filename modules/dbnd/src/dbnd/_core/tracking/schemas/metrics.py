import json

import attr

from dbnd._core.constants import _DbndDataClass


class Metric(_DbndDataClass):
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
        if value_json:
            self.value_json = value_json
            self.value_str = None
        elif not (value_int or value_float or value_str):
            self.value = value

    @property
    def value(self):
        if self.value_float is not None:
            return self.value_float
        if self.value_int is not None:
            return self.value_int
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
        if isinstance(value, float):
            self.value_float = value
        elif isinstance(value, int):
            self.value_int = value
        else:
            self.value_str = value

    def __repr__(self):
        return "Metric(key={}, source={})".format(self.key, self.source)


@attr.s
class Artifact(_DbndDataClass):
    path = attr.ib()  # type: str
