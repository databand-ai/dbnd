import inspect
import itertools
import logging
import re
import typing

from typing import List, Optional

from dbnd._vendor.pytypes.type_utils import get_Union_params, is_Union
from targets.types import EagerLoad, LazyLoad
from targets.values.builtins_values import DefaultObjectValueType
from targets.values.structure import _StructureValueType
from targets.values.value_type import InlineValueType, ValueType


def f_name(f):
    return f.__name__


logger = logging.getLogger(__name__)


class ValueTypeRegistry(object):
    def __init__(self, known_value_types):
        self.value_types = []  # type: List[ValueType]
        self.discoverable_value_types = []  # type: List[ValueType]
        self.default = DefaultObjectValueType()

        # now for every parameter we also have text representation of the type
        # will be used for annotations
        self.type_str_to_parameter = {}
        for value_type in known_value_types:
            self.register_value_type(value_type)

        self._type_handler_from_type = TypeHandlerFromType(self)
        self._type_handler_from_type_str = TypeHandlerFromDocAnnotation(self)

    def register_value_type(self, value_type):
        self.value_types.append(value_type)
        if value_type.discoverable:
            self.discoverable_value_types.append(value_type)
        try:
            # for t in [value_type.type]:

            type_str = value_type.type_str
            if type_str:
                self.type_str_to_parameter[type_str] = value_type
                if type_str.startswith("typing"):
                    self.type_str_to_parameter[
                        type_str.replace("typing.", "")
                    ] = value_type
                if " " in type_str:
                    self.type_str_to_parameter[type_str.replace(" ", "")] = value_type
            if value_type.type_str_extras:
                for t in value_type.type_str_extras:
                    self.type_str_to_parameter[t] = value_type
        except Exception as ex:
            raise Exception("Failed to process %s: %s" % (value_type, ex))
        return value_type

    def get_value_type_of_obj(self, value, default=None):
        # type: (object, Optional[ValueType]) -> Optional[ValueType]
        # do we want to automatically parse str_list?
        # right now we do that, but as for obj_list
        # we can keep deterministic conversion only

        for item in self.discoverable_value_types:
            if item.is_type_of(value):
                return item
        return default

    def get_value_type_of_type(self, type_, inline_value_type=False):
        if isinstance(type_, ValueType):
            return type_
        elif isinstance(type_, type) and issubclass(type_, ValueType):
            return type_()
        return self._type_handler_from_type.get_value_type_of_type(
            type_=type_, inline_value_type=inline_value_type
        )

    def get_value_type_of_type_str(self, type_str):
        # type: (str) -> Optional[ValueType]
        return self._type_handler_from_type_str.get_value_type_of_type_str(type_str)

    def list_known_types(self):
        return list(self.type_str_to_parameter.keys())


def get_non_generic_type(type_):
    if isinstance(type_, type):
        return type_
    else:
        if hasattr(type_, "__extra__"):  # py 3.5/3.6
            return type_.__extra__
        elif hasattr(type_, "__origin__"):  # py 3.7
            return type_.__origin__
        else:
            return None


class TypeHandlerFromType(object):
    def __init__(self, registry):
        self.registry = registry  # type: ValueTypeRegistry

    def _get_value_type_of_type(self, type_, inline_value_type=False):
        if isinstance(
            type_, typing.TypeVar
        ):  # placeholder type for unparametrized generic type
            return None

        # from py3.7 issubclass(typing.List[str], typing.List) raises an exception, so we extract the simple type first
        non_generic_type = get_non_generic_type(type_)

        for value_type in self.registry.value_types:
            if value_type.type == type_:
                return value_type

            if non_generic_type is None:
                continue

            if isinstance(non_generic_type, type) and value_type.is_handler_of_type(
                non_generic_type
            ):
                return value_type
        if inline_value_type:
            return InlineValueType(type_)
        return None

    def get_value_type_of_type(self, type_, inline_value_type=False):
        lazy_load = None

        if is_Union(type_):
            union_params = get_Union_params(type_)
            if LazyLoad in union_params:
                lazy_load = True
            elif EagerLoad in union_params:
                lazy_load = False
            # take only first in Union
            type_ = union_params[0]
        if inspect.isclass(type_) and issubclass(type_, LazyLoad):
            lazy_load = True

        type_handler = self._get_value_type_of_type(
            type_, inline_value_type=inline_value_type
        )
        if not type_handler:
            return type_handler

        if isinstance(type_handler, _StructureValueType):
            if type_handler.type == typing.Dict:
                sub_type = self.get_dict_value_type(type_)
            else:
                sub_type = self.get_generic_item_type(type_)

            if sub_type:
                sub_type_handler = self.registry.get_value_type_of_type(
                    sub_type, inline_value_type=inline_value_type
                )
                type_handler = type_handler.with_sub_type_handler(sub_type_handler)
        if lazy_load is not None:
            type_handler = type_handler.with_lazy_load(lazy_load=lazy_load)
        return type_handler

    def get_dict_value_type(self, type_):
        args = _get_generic_params(type_)
        if args is not None and len(args) == 2:
            return args[1]
        return None

    def get_generic_item_type(self, type_):
        args = _get_generic_params(type_)
        if args is not None and len(args) == 1:
            return args[0]
        return None


def _get_generic_params(type_):
    if hasattr(type_, "__args__"):
        args = type_.__args__
        if isinstance(args, tuple) and args:
            return args
    return None


class TypeHandlerFromDocAnnotation(object):
    # re:   Optional[ Type1 ]
    _OPTIONAL_TYPE_RE = re.compile(r"^Optional\[(.+)\]$")
    # re:   Union[ Type1, .. ]
    _UNION_TYPE_RE = re.compile(r"^Union\[(.+)\]$")

    # re:   Union[ Type1, .. ]
    _SIMPLE_TYPE_RE = re.compile(r"^[^(\[]+\.(.+)$")
    # re:   Union[ Type1, .. ]
    _GENERIC_TYPE_RE = re.compile(r"^([^\[]+)\[(.+)\]$")

    _SPLIT_ARGS_RE = re.compile(r",(?![^()\[\]]*[)\]])")

    def __init__(self, registry):
        self.registry = registry  # type: ValueTypeRegistry

    def _get_value_type_of_type_str(self, type_str):
        r = self.registry
        options = [type_str, type_str.lower()]
        # let take only "last" part of   some_module.SomeType[]
        simple_type_match = self._SIMPLE_TYPE_RE.match(type_str)
        if simple_type_match:
            simple_type_str = simple_type_match.group(1)
            options += [simple_type_str, simple_type_str.lower()]

        options = [x[0] for x in itertools.groupby(options)]

        # we are looking for the exact match
        for ts in options:
            type_handler = r.type_str_to_parameter.get(ts)
            if type_handler:
                return type_handler

        # now we can try "generic"
        for ts in options:
            generic_match = self._GENERIC_TYPE_RE.match(ts)
            if not generic_match:
                continue
            main_type = generic_match.group(1)
            generic_part = generic_match.group(2)

            type_handler = r.type_str_to_parameter.get(main_type)
            if type_handler:
                if type_handler.type == typing.Dict:
                    parts = self._SPLIT_ARGS_RE.split(generic_part)
                    if len(parts) == 2:
                        generic_part = parts[1]
                    else:
                        continue

                sub_type_handler = r.get_value_type_of_type_str(generic_part)
                return type_handler.with_sub_type_handler(sub_type_handler)

        return None

    def get_value_type_of_type_str(self, type_str):
        # type: (str) -> Optional[ValueType]
        if not type_str:
            return None
        # if not REGEX_SIMPLE_TYPE.match(type_str):
        #     return None
        type_str = type_str.replace(" ", "")
        lazy_load = None
        # we remove Optional wrapping - we don't use that information
        optional_def = self._OPTIONAL_TYPE_RE.search(type_str)
        if optional_def:
            type_str = optional_def.group(1)

        union = self._UNION_TYPE_RE.search(type_str)
        if union:
            type_str = union.group(1)
            # we take only first argument on Union
            union_types = self._SPLIT_ARGS_RE.split(type_str)
            if "LazyLoad" in union_types:
                lazy_load = True
            type_str = union_types[0]

        if "DataList" in type_str:
            lazy_load = True

        type_handler = self._get_value_type_of_type_str(type_str)
        if not type_handler:
            return None

        if lazy_load is not None:
            type_handler = type_handler.with_lazy_load(lazy_load)

        return type_handler
