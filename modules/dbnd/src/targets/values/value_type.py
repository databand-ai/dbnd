import abc
import copy
import hashlib
import logging
import re

from typing import Any, Optional, Tuple, Union

import six

from dbnd._core.errors import friendly_error
from dbnd._core.utils import json_utils
from dbnd._core.utils.basics.load_python_module import run_user_func
from dbnd._vendor import fast_hasher
from targets.config import get_value_preview_max_len
from targets.target_meta import TargetMeta


logger = logging.getLogger(__name__)
_IS_INSTANCE_SAMPLE = 1000


class ValueType(object):
    support_merge = False
    support_from_str = True

    type_str_extras = None

    load_on_build = True
    discoverable = True  # we can discover it from the object

    @property
    @abc.abstractmethod
    def type(self):
        pass

    @property
    def type_str(self):
        if hasattr(self.type, "__name__"):
            s = str(self.type.__name__)
        else:
            s = str(self.type)

        return re.sub(r"^typing\.", "", s)

    @property
    def config_name(self):
        return "object"
        # if self.type is None:
        #     raise Exception("Value type doesn't have a name! %s" % self)
        # if hasattr(self.type, "__name__"):
        #     return str(self.type.__name__)
        # return str(self.type)

    def merge_values(self, *values, **kwargs):
        pass

    def is_type_of(self, value):
        return self.type is not None and type(value) == self.type

    def is_handler_of_type(self, type_):
        return self.type is not None and self.type == type_

    #################
    # transformations
    #################
    def normalize(self, x):  # type: (T) -> T
        """
        Given a parsed parameter value, normalizes it.

        The value can either be the result of parse(), the default value or
        arguments passed into the task's constructor by instantiation.

        This is very implementation defined, but can be used to validate/clamp
        valid values. For example, if you wanted to only accept even integers,
        and "correct" odd values to the nearest integer, you can implement
        normalize as ``x // 2 * 2``.
        """
        return x  # default impl

    def parse_from_str(self, x):  # type: (str) -> T
        if not self.support_from_str:
            raise friendly_error.targets.type_without_parse_from_str(self)
        return x  # default impl

    def parse_from_str_lines(self, lines):
        return self.parse_from_str("".join(lines))

    def parse_if_str(self, x):  # type: (Union[str, T]) -> T
        if isinstance(x, six.string_types):
            return self.parse_from_str(x)
        return x

    def _generate_empty_default(self):
        return None

    # dump

    def to_str(self, x):  # type: (T) -> str
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return str(x)

    def to_str_lines(self, x):
        return self.to_str(x)

    def to_repr(self, x):
        return repr(x)

    def to_signature(self, x):
        return self.to_str(x)

    def to_preview(self, x):  # type: (T) -> str
        return self.to_str(x)[: get_value_preview_max_len()]

    ##################
    # Target I/O
    ##################

    def _interpolate_from_str(self, value):
        from dbnd._core.utils.task_utils import to_targets

        # when we parse string, if it's has @ - we always will want to "de-reference" it
        # and read from disk
        # it's very important for primitive types, as we don't know
        # if we need to parse("str") or read_from_disk("str")
        # @ in the beginning of the string supported by @@
        if isinstance(value, six.string_types) and value.startswith("@"):
            value = value[1:]
            if value.startswith("@"):
                # this is the way to provide string value with @
                pass
            else:
                # so this is @/somepath case and not @@
                # TODO: we need to move it into target wrapper
                if value.startswith("python://"):
                    value = value[9:]
                    value = run_user_func(value)
                else:
                    from targets import target

                    value = self.load_from_target(target(value))
        return value

    def parse_value(self, value, load_value=None, target_config=None):
        """
        Parse an individual value from the input.

        probably this is the most important code in user value parsing
        :param str value: the value to parse.
        :return: the parsed value.
        """

        from dbnd._core.utils.task_utils import to_targets
        from targets.inmemory_target import InMemoryTarget
        from targets.values.target_values import _TargetValueType

        if load_value is None:
            load_value = self.load_on_build

        value = self._interpolate_from_str(value)
        if value is None:
            return value

        if isinstance(value, six.string_types):
            # we are in the string mode
            # it's can be "serialized to string" or path value
            if load_value:
                # we can just load value from string
                if self.support_from_str:
                    value = self.parse_from_str(value)
                    value = self.normalize(value)
                    return value

            # otherwise - the value is a path!
            target_kwargs = {}
            if target_config:
                target_kwargs["config"] = target_config

            return to_targets(json_utils.loads(value), from_string_kwargs=target_kwargs)

        from dbnd._core.task import Task
        from targets import Target

        if isinstance(value, Task):
            return to_targets(value)

        if isinstance(value, Target):
            return value

        # so we have a value that is obviously "Data" type,
        # we want to be able to supporet "load_value" behaviour
        if not load_value and not isinstance(self, _TargetValueType):
            return InMemoryTarget(value, value_type=self)

        value = self.normalize(value)
        return value

    def load_runtime(self, value):
        if not value:
            return value

        from targets import InMemoryTarget, DataTarget

        if isinstance(value, InMemoryTarget):
            return value.load()
        if isinstance(value, DataTarget):
            return self.load_from_target(value)
        return value

    def load_from_target(self, target, **kwargs):
        # type: (DataTarget, **Any) -> Any
        return target.load(value_type=self, **kwargs)

    def save_to_target(
        self, target, value, **kwargs
    ):  # type: (DataTarget, T, **Any)-> None
        target.dump(value, value_type=self, **kwargs)

    #####################
    # Utilities
    #####################
    def next_in_enumeration(self, _value):
        """
        If your Parameter type has an enumerable ordering of values. You can
        choose to override this method. This method is used by the
        :py:mod:`databand.execution_summary` module for pretty printing
        purposes. Enabling it to pretty print tasks like ``MyTask(num=1),
        MyTask(num=2), MyTask(num=3)`` to ``MyTask(num=1..3)``.

        :param value: The value
        :return: The next value, like "value + 1". Or ``None`` if there's no enumerable ordering.
        """
        return None

    def __call__(self):
        """
        support putting instance of ValueType into Generic
        for example List[ValueType()]
        :return:
        """
        return self

    def __repr__(self):
        return "{self.type_str}@{self.__class__.__name__}".format(self=self)

    def __str__(self):
        return "{self.type_str}".format(self=self)

    def with_lazy_load(self, lazy_load=True):
        # a little bit confusing
        # load on build == not lazy_load
        if self.load_on_build != lazy_load:
            return self

        # copy doesn't work - bug in python generics
        new_self = copy.deepcopy(self)
        new_self.load_on_build = not lazy_load
        return new_self

    def with_sub_type_handler(self, type_handler):
        return self

    def get_data_dimensions(self, value):  # type: (Any) -> Optional[Tuple[int, ...]]
        return None

    def get_data_schema(self, value):  # type: (Any) -> str
        return json_utils.dumps({"type": self.type_str})

    def get_data_hash(self, value):
        return fast_hasher.hash(value)

    def get_value_meta(self, value, with_preview=True):
        data_dimensions = self.get_data_dimensions(value)
        if data_dimensions is not None:
            data_dimensions = list(data_dimensions)
        preview = self.to_preview(value) if with_preview else None
        data_schema = self.get_data_schema(value)
        data_hash = self.get_data_hash(value)

        return TargetMeta(
            value_preview=preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=data_hash,
        )


class InlineValueType(ValueType):
    support_from_str = False

    def __init__(self, type_):
        self._type = type_

    @property
    def type(self):
        return self._type

    @property
    def type_str(self):
        return self._type.__name__


def _isinstances(list_obj, type_):
    if not list_obj:
        return True
    if _IS_INSTANCE_SAMPLE and len(list_obj) > _IS_INSTANCE_SAMPLE:
        # we don't want to run isinstance on 1M objects..
        count = 0
        for elem in list_obj:
            if not isinstance(elem, type_):
                return False
            if count > _IS_INSTANCE_SAMPLE:
                return True
            count += 1
        return True

    return all(isinstance(elem, type_) for elem in list_obj)
