# © Copyright Databand.ai, an IBM Company 2022

"""
We want to define new custom object so we can run code like :
  dbnd run my_task --custom "some custom text for MyCustomObject constuctor"

"""

import logging

from typing import Dict, List, Set

from dbnd import PythonTask, output, parameter
from targets.values import ValueType, register_value_type


logger = logging.getLogger(__name__)


class MyCustomObject(object):
    def __init__(self, custom):
        self.custom = custom


class _MyCustomObjectValueType(ValueType):
    type = MyCustomObject

    def parse_from_str(self, x):
        return MyCustomObject(x)

    def to_str(self, x):
        return x.custom


register_value_type(_MyCustomObjectValueType())


class TaskWithCustomValue(PythonTask):
    custom_value = parameter.type(_MyCustomObjectValueType)
    list_of_customs = parameter.sub_type(_MyCustomObjectValueType)[List]

    report = output[str]

    def run(self):
        assert isinstance(self.custom_value, MyCustomObject)
        assert isinstance(self.list_of_customs[1], MyCustomObject)
        self.report = self.custom_value.custom + self.list_of_customs[1].custom


# We register this type, so now we can just use the .type -> MyCustomObject

register_value_type(_MyCustomObjectValueType())


class TaskWithCustomValueInline(PythonTask):
    # works only after registration!
    custom_value = parameter[MyCustomObject]
    custom_value_with_default = parameter.value(MyCustomObject("1"))

    list_of_customs = parameter.sub_type(MyCustomObject)[List]
    set_of_customs = parameter.sub_type(MyCustomObject)[Set]
    dict_of_customs = parameter.sub_type(MyCustomObject)[Dict]

    report = output[str]

    def run(self):
        assert isinstance(self.custom_value, MyCustomObject)
        assert isinstance(self.custom_value_with_default, MyCustomObject)

        self.report = "_".join(
            [
                c.custom
                for c in [
                    self.custom_value,
                    self.custom_value_with_default,
                    self.list_of_customs[0],
                    self.dict_of_customs["a"],
                ]
            ]
        )
