# © Copyright Databand.ai, an IBM Company 2022

import functools
import itertools
import json
import logging
import typing

from ast import literal_eval
from collections import OrderedDict
from functools import partial
from typing import Iterable

import six

from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils import json_utils
from dbnd._core.utils.traversing import traverse
from dbnd._vendor.splitter import split_args, unquote
from targets.value_meta import ValueMeta
from targets.values.value_type import ValueType


logger = logging.getLogger(__name__)

_PARSABLE_PARAM_PREFIX = "@{["


#
# if issubclass(obj_type, (typing.List, list)) or obj_type == typing.Sequence:
#     return self._structured_type(obj_type, ListValueType)
# if issubclass(obj_type, (typing.Dict, dict)):
#     return self._structured_type(obj_type, DictValueType)
# if issubclass(obj_type, (typing.Tuple, tuple)):
#     return self._structured_type(obj_type, TupleValueType)


class _StructureValueType(ValueType):
    sub_value_type = None

    def __init__(self, sub_value_type=None):
        super(_StructureValueType, self).__init__()
        self.sub_value_type = sub_value_type or self.__class__.sub_value_type

    def is_type_of(self, value):
        return isinstance(value, self.type)

    def is_handler_of_type(self, type_):
        return issubclass(type_, self.type)

    def parse_from_str(self, x):
        """
               Parses an immutable and ordered ``dict`` from a JSON string using standard JSON library.
        Parse an individual value from the input.

        """

        # if isinstance(value, Mapping):
        #     # we are good to go, it'x dictionary already
        #     return value
        if not x:
            return self._generate_empty_default()

        # this is string and we need to parse it
        if not isinstance(x, six.string_types):
            raise DatabandConfigError(
                "Can't parse '%x' into parameter. Value should be string" % x
            )

        x = x.strip()
        if not x:
            return self._generate_empty_default()

        if x[0] in _PARSABLE_PARAM_PREFIX:
            value = json_utils.loads(x)
        else:
            value = self._parse_from_str_simple(x)

            if not self.is_type_of(value):
                raise DatabandConfigError(
                    "Can't parse '%s' into %s" % (value, self.type)
                )
        if self.sub_value_type:
            value = traverse(value, self.sub_value_type.parse_value)

        return value

    def parse_from_str_lines(self, lines):
        value = lines
        if self.sub_value_type:
            value = traverse(value, self.sub_value_type.parse_from_str)
        return value

    def normalize(self, value):
        if self.sub_value_type:
            value = traverse(value, self.sub_value_type.normalize)
        return value

    def parse_value(self, value, load_value=None, target_config=None, sub_value=False):
        """
        parse structure first
        parse every element
        """
        if value is None:
            return value

        if not sub_value and isinstance(value, six.string_types):
            return super(_StructureValueType, self).parse_value(
                value=value, load_value=load_value, target_config=target_config
            )

        if self.sub_value_type:
            return traverse(
                struct=value,
                convert_f=partial(self.sub_value_type.parse_value, sub_value=True),
            )

        return value

    def load_runtime(self, value, **kwargs):
        if self.sub_value_type:
            return traverse(value, self.sub_value_type.load_runtime)
        return value

    def _parse_from_str_simple(self, str_value):
        raise NotImplementedError("non parsable inputs are not supported for %s" % self)

    def to_str(self, x):
        if self.sub_value_type:
            x = traverse(x, self.sub_value_type.to_str)
        return json_utils.dumps_safe(x)

    def to_str_lines(self, x):
        if self.sub_value_type:
            x = traverse(x, self.sub_value_type.to_str)
        return x

    def with_sub_type_handler(self, type_handler):
        # bug in python <3.7  (copy of generic)
        # new_self = copy.copy(self)
        # new_self.sub_value_type = type_handler
        # return new_self
        new_value_type = self.__class__(sub_value_type=type_handler)
        new_value_type.marshallers.update(self.marshallers)
        return new_value_type

    def __iter__(self):
        raise Exception("Do not iterate, this one is for type hinting only")

    def __repr__(self):
        sub_value_type_repr = ""
        if self.sub_value_type:
            sub_value_type_repr = "[{}]".format(repr(self.sub_value_type))
        return "{self.type_str}{sub_value_type_repr}@{self.__class__.__name__}".format(
            self=self, sub_value_type_repr=sub_value_type_repr
        )

    def __str__(self):
        sub_value_type_repr = ""
        if self.sub_value_type:
            sub_value_type_repr = "[{}]".format(str(self.sub_value_type))
        return "{self.type_str}{sub_value_type_repr}".format(
            self=self, sub_value_type_repr=sub_value_type_repr
        )

    def load_value_type(self):
        """
        simplifies ValueTypeLoader implementation,
        we don't need to check if we need to "load every time we use it
        """
        if self.sub_value_type:
            self.sub_value_type = self.sub_value_type.load_value_type()
        return self


class DictValueType(_StructureValueType):
    """
    Parameter whose value is a ``dict``.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
            tags = databand.DictParameter()

            def run(self):
                logging.info("Find server with role: %s", self.tags['role'])
                server = aws.ec2.find_my_resource(self.tags)


    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --tags <JSON string>
        $ dbnd --module my_tasks MyTask --tags 'key=value key="some value"'

    Simple example with two tags:

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --tags '{"role": "web", "env": "staging"}'

    We also enable to provide multiple values per parameter

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --tags '{"role": "web"}'  --tags  '{ "env": "staging"}'


    It can be used to define dynamic parameters, when you do not know the exact list of your parameters (e.g. list of
    tags, that are dynamically constructed outside databand), or you have a complex parameter containing logically related
    values (like a database connection config).
    """

    type = typing.Dict

    type_str_extras = ("dict",)
    support_merge = True

    def merge_values(self, *values, **kwargs):
        return functools.reduce(lambda x, y: dict(x, **y), values)

    def _generate_empty_default(self):
        return dict()

    def _parse_from_str_simple(self, str_value):
        p_value = {}
        for param in split_args(str_value):
            if "=" not in param:
                raise ValueError(
                    "Failed to parse '%s', expected format is KEY=VALUE", param
                )
            name, sep, var = param.partition("=")
            p_value[name.strip()] = unquote(var.strip())
        return p_value

    def __str__(self):
        sub_value_type_repr = ""
        if self.sub_value_type:
            sub_value_type_repr = "[Any,{}]".format(str(self.sub_value_type))
        return "{self.type_str}{sub_value_type_repr}".format(
            self=self, sub_value_type_repr=sub_value_type_repr
        )


class ListValueType(_StructureValueType, Iterable):
    """
    Value whose value is a ``list``.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
          grades = databand.ListParameter()

            def run(self):
                sum = 0
                for element in self.grades:
                    sum += element
                avg = sum / len(self.grades)


    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --grades <JSON string>
        $ dbnd --module my_tasks MyTask --grades '[100,70]'

    """

    DEFAULT_MAX_ELEMENTS_TO_PREVIEW = 10
    type = typing.List
    type_str_extras = ("list", "DataList")

    config_name = "list"
    support_merge = True  # we know how to merge frames

    def get_value_meta(self, value, meta_conf):
        # type: (list, ValueMetaConf) -> ValueMeta
        data_schema = self.get_list_metrics(value, meta_conf)
        data_dimensions = data_schema.get("shape")
        if meta_conf.log_size:
            data_schema["size.bytes"] = value.__sizeof__()

        value_preview, data_hash = None, None
        if meta_conf.log_preview:
            value_preview = self.to_preview(
                value, preview_size=self.get_preview_size(meta_conf)
            )
            try:
                data_hash = hash(json.dumps(value))
            except Exception as e:
                logger.warning("Could not hash list %s! Exception: %s", value, e)

        # calculating stats, metrics and histograms are out of scope at the moment
        stats, histograms = [], {}
        hist_sys_metrics = None

        return ValueMeta(
            value_preview=value_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema if meta_conf.log_schema else None,
            data_hash=data_hash,
            columns_stats=stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )

    def get_list_metrics(self, value, meta_conf):
        # type: (list, ValueMetaConf)-> dict
        """
        caculates list schema, in case of dict objects aggregate total columns, types and dimension of all objects in list

        """
        data_schema = {}
        columns = set()
        dtypes = {}
        for obj in value:
            if isinstance(obj, dict):
                flat_obj = json_utils.flatten_dict(obj)
                columns.update(flat_obj.keys(), obj.keys())
                if meta_conf.log_schema:
                    self.aggregate_object_column_types(dtypes, flat_obj, obj)
        # sort list of columns to prevent schema order change
        columns_sort = list(columns)
        columns_sort.sort()
        data_schema.update(
            {
                "type": self.type_str,
                "columns": columns_sort,
                "shape": (len(value), len(columns)),
                "dtypes": dtypes,
            }
        )
        return data_schema

    def aggregate_object_column_types(
        self, dtypes: dict, flat_obj: dict, orig_obj: dict
    ):
        for key, val in flat_obj.items():
            if key not in dtypes or val is not None:
                dtypes[key] = str(type(val))
        # add types of nested fields from original object
        for key, val in orig_obj.items():
            if isinstance(val, (list, dict)):
                dtypes[key] = str(type(val))

    def get_preview_size(self, meta_conf):
        # type: (ValueMetaConf)-> int
        conf_preview_size = meta_conf.get_preview_size()
        return (
            conf_preview_size
            if conf_preview_size < self.DEFAULT_MAX_ELEMENTS_TO_PREVIEW
            else self.DEFAULT_MAX_ELEMENTS_TO_PREVIEW
        )

    def _generate_empty_default(self):
        return list()

    def _parse_from_str_simple(self, value):
        return value.split(",")

    def merge_values(self, *values):
        return list(itertools.chain(*values))

    def to_preview(self, x, preview_size):  # type: (list, int) -> str
        try:
            return json.dumps(x[:preview_size])
        except Exception:
            logger.exception(f"Failed to convert list to json string: {x}")


class SetValueType(_StructureValueType):
    """
    Value whose value is a ``set``.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
          grades = databand.ListParameter()

            def run(self):
                sum = 0
                for element in self.grades:
                    sum += element
                avg = sum / len(self.grades)


    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --grades <JSON string>

    Simple example with two grades:

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --grades '[100,70]'
    """

    type = typing.Set

    def _generate_empty_default(self):
        return set()

    def _parse_from_str_simple(self, value):
        return set(value.split(","))

    def normalize(self, value):
        if isinstance(value, set):
            return value
        return set(value)

    def to_str(self, x):
        if self.sub_value_type:
            x = traverse(x, self.sub_value_type.to_str)

        # we sort the set before we serialize!
        x = sorted(x, key=lambda x: json_utils.dumps_canonical(x))
        return json_utils.dumps_canonical(x)


class TupleValueType(_StructureValueType):
    """
    Parameter whose value is a ``tuple`` or ``tuple`` of tuples.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
          book_locations = databand.TupleParameter()

            def run(self):
                for location in self.book_locations:
                    print("Go to page %d, line %d" % (location[0], location[1]))


    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --book_locations <JSON string>

    Simple example with two grades:

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --book_locations '((12,3),(4,15),(52,1))'
    """

    type = typing.Tuple

    type_str_extras = ("tuple",)

    config_name = "tuple"

    def _parse_from_str_simple(self, value):
        return value.split(",")

    def _generate_empty_default(self):
        return tuple()

    def parse_from_str(self, x):
        """
        Parse an individual value from the input.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        # Since the result of json.dumps(tuple) differs from a tuple string, we must handle either case.
        # A tuple string may come from a config file or from cli execution.

        # t = ((1, 2), (3, 4))
        # t_str = '((1,2),(3,4))'
        # t_json_str = json.dumps(t)
        # t_json_str == '[[1, 2], [3, 4]]'
        # json.loads(t_json_str) == t
        # json.loads(t_str) == ValueError: No JSON object could be decoded

        # Therefore, if json.loads(x) returns a ValueError, try ast.literal_eval(x).
        # ast.literal_eval(t_str) == t
        # hjson will not handle that!
        try:
            # loop required to parse tuple of tuples
            return tuple(tuple(x) for x in json.loads(x, object_pairs_hook=OrderedDict))
        except ValueError:
            return literal_eval(x)  # if this causes an error, let that error be raised.

    def is_handler_of_type(self, type_):
        return type_ in [typing.Tuple, tuple]
