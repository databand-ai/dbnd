# © Copyright Databand.ai, an IBM Company 2022

import inspect
import itertools
import logging
import re
import typing

from typing import Dict, List, Optional

import six

from dbnd._vendor.pytypes.type_utils import get_Union_params, is_Union
from targets.types import EagerLoad, LazyLoad
from targets.values.builtins_values import DefaultObjectValueType
from targets.values.structure import _StructureValueType
from targets.values.value_type import InlineValueType, ValueType
from targets.values.value_type_loader import ValueTypeLoader


def f_name(f):
    return f.__name__


logger = logging.getLogger(__name__)


class ValueTypeRegistry(object):
    def __init__(self):
        self.value_types: List[ValueType] = []
        self._class_name_to_value_type: Dict[str, ValueType] = {}

        self.value_types_from_obj: List[ValueType] = []
        self.default = DefaultObjectValueType()

        self._type_handler_from_type = TypeHandlerFromType(self)
        self._type_handler_from_type_str = TypeHandlerFromDocAnnotation(self)

    def register_value_type(self, value_type):
        value_type = self.get_value_type_of_type(value_type, inline_value_type=True)

        self.value_types.append(value_type)
        if value_type.support_discover_from_obj:
            self.value_types_from_obj.append(value_type)
        try:
            # for t in [value_type.type]:

            type_str = value_type.type_str
            if type_str:
                self._class_name_to_value_type[type_str] = value_type
                if type_str.startswith("typing"):
                    self._class_name_to_value_type[
                        type_str.replace("typing.", "")
                    ] = value_type
                if " " in type_str:
                    self._class_name_to_value_type[
                        type_str.replace(" ", "")
                    ] = value_type
            if value_type.type_str_extras:
                for t in value_type.type_str_extras:
                    self._class_name_to_value_type[t] = value_type
        except Exception as ex:
            raise Exception("Failed to process %s: %s" % (value_type, ex))
        return value_type

    def get_value_type_of_obj(self, value, default=None):
        # type: (object, Optional[ValueType]) -> Optional[ValueType]
        # do we want to automatically parse str_list?
        # right now we do that, but as for obj_list
        # we can keep deterministic conversion only
        for value_type in self.value_types_from_obj:
            if value_type.is_type_of(value):
                # we are working with runtime objects, we should load value type definitions at this point
                loaded_value_type = value_type.load_value_type()
                if loaded_value_type is None:
                    continue
                return loaded_value_type
        return default

    def get_value_type_of_type(self, type_, inline_value_type=False):
        if isinstance(type_, ValueType):
            return type_
        if isinstance(type_, ValueTypeLoader):
            return type_

        if isinstance(type_, type) and issubclass(type_, ValueType):
            return type_()

        # we should have real value type of None here
        if isinstance(type_, six.string_types):
            return self._type_handler_from_type_str.get_value_type_of_type_str(
                type_str=type_
            )
        return self._type_handler_from_type.get_value_type_of_type(
            type_=type_, inline_value_type=inline_value_type
        )

    def get_value_type_of_type_str(self, type_str):
        # type: (str) -> Optional[ValueType]
        return self._type_handler_from_type_str.get_value_type_of_type_str(type_str)

    def find_value_type_of_class(self, class_):
        # from py3.7 issubclass(typing.List[str], typing.List) raises an exception, so we extract the simple type first
        class_without_generic = get_non_generic_type(class_)

        for value_type in self.value_types:
            # regular types can handle Generic by ==, however, ValueTypeLoader need to call is_handler_of_type
            if value_type.type and value_type.type == class_:
                return value_type

            if class_without_generic != class_ and isinstance(
                value_type, ValueTypeLoader
            ):
                if value_type.is_handler_of_type(class_):
                    return value_type

            # handle generic and non generic types
            if class_without_generic is not None:
                if value_type.is_handler_of_type(class_without_generic):
                    return value_type
        return None

    def list_known_types(self):
        return list(self._class_name_to_value_type.keys())


def get_non_generic_type(type_):
    if isinstance(type_, type):
        return type_
    elif hasattr(type_, "__extra__") and isinstance(
        type_.__extra__, type
    ):  # py 3.5/3.6
        return type_.__extra__
    elif hasattr(type_, "__origin__") and isinstance(type_.__origin__, type):  # py 3.7
        return type_.__origin__
    return None


class TypeHandlerFromType(object):
    def __init__(self, registry):
        self.registry = registry  # type: ValueTypeRegistry

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

        if isinstance(
            type_, typing.TypeVar
        ):  # placeholder type for unparametrized generic type
            return None

        type_handler = self.registry.find_value_type_of_class(class_=type_)
        if type_handler is not None:
            if isinstance(type_handler, _StructureValueType):
                if type_handler.type == typing.Dict:
                    sub_type = self.get_dict_value_type(type_)
                else:
                    sub_type = self.get_generic_item_type(type_)

                if sub_type:
                    # recursion for sub-type (only for structured)
                    sub_type_handler = self.registry.get_value_type_of_type(
                        sub_type, inline_value_type=inline_value_type
                    )
                    if sub_type_handler:
                        type_handler = type_handler.with_sub_type_handler(
                            sub_type_handler
                        )
        elif inline_value_type:
            type_handler = InlineValueType(type_)
        else:
            return None

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
            type_handler = r._class_name_to_value_type.get(ts)
            if type_handler:
                return type_handler

        # now we can try "generic"
        for ts in options:
            generic_match = self._GENERIC_TYPE_RE.match(ts)
            if not generic_match:
                continue
            main_type = generic_match.group(1)
            generic_part = generic_match.group(2)

            type_handler = r._class_name_to_value_type.get(main_type)
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
