import logging

import attr
import numpy
import pandas as pd

from dbnd._core.configuration.config_path import ConfigPath
from dbnd._core.constants import OutputMode, _ConfigParamContainer
from dbnd._core.errors import friendly_error
from dbnd._core.errors.friendly_error.task_parameters import (
    failed_to_build_parameter,
    unknown_value_type_in_parameter,
    value_is_not_parameter_cls,
)
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _add_description,
    _ParameterKind,
)
from dbnd._core.parameter.validators import ChoiceValidator, NumericalValidator
from dbnd._core.utils.basics.nothing import NOTHING, is_defined, is_not_defined
from targets.target_config import TargetConfig
from targets.types import DataList
from targets.value_meta import ValueMetaConf
from targets.values import (
    TargetPathValueType,
    TargetValueType,
    ValueType,
    get_value_type_of_obj,
    get_value_type_of_type,
)
from targets.values.builtins_values import EnumValueType
from targets.values.structure import _StructureValueType
from targets.values.value_type import InlineValueType


logger = logging.getLogger(__name__)


class ParameterFactory(object):
    def __init__(self, parameter=None):
        self.parameter = parameter or ParameterDefinition()

    def modify(self, **kwargs):
        if not kwargs:
            return self
        new_parameter = attr.evolve(self.parameter, **kwargs)
        return ParameterFactory(parameter=new_parameter)

    def value(self, default=NOTHING, **kwargs):
        """
        Constructs parameter with default=Value
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool significant: specify ``False`` if the parameter should not be treated as part of
                                 the unique identifier for a Task. An insignificant Parameter might
                                 also be used to specify a password or other sensitive information
                                 that should not be made public. Default:
                                 ``True``.
        :param str description: A human-readable string describing the purpose of this Parameter.
                                For command-line invocations, this will be used as the `help` string
                                shown to users. Default: ``None``.
        :param OutputMode output_mode: specify OutputMode.prod_immutable to convert this parameter
                                to production immutable output
        :return: ParameterFactory
        """
        return self.modify(default=default, **kwargs)

    def __call__(self, **kwargs):
        """
        Changes parameter behaviour
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool significant: specify ``False`` if the parameter should not be treated as part of
                                 the unique identifier for a Task. An insignificant Parameter might
                                 also be used to specify a password or other sensitive information
                                 that should not be made public. Default:
                                 ``True``.
        :param str description: A human-readable string describing the purpose of this Parameter.
                                For command-line invocations, this will be used as the `help` string
                                shown to users. Default: ``None``.
        :param OutputMode output_mode: specify OutputMode.prod_immutable to convert this parameter
                                to production immutable output
        :return: ParameterFactory
        """
        return self.modify(**kwargs)

    @property
    def none(self):
        return self.modify(default=None)

    ########
    # formats
    def target_config(self, target_config):
        return self.modify(target_config=target_config)

    @property
    def _target_config(self):
        return self.parameter.target_config or TargetConfig()

    @property
    def gz(self):
        return self.target_config(self._target_config.gzip)

    @property
    def gzip(self):
        return self.target_config(self._target_config.gzip)

    @property
    def parquet(self):
        return self.target_config(self._target_config.parquet)

    @property
    def json(self):
        return self.target_config(self._target_config.json)

    @property
    def hdf5(self):
        return self.target_config(self._target_config.hdf5)

    @property
    def txt(self):
        return self.target_config(self._target_config.txt)

    @property
    def csv(self):
        return self.target_config(self._target_config.csv)

    @property
    def tsv(self):
        return self.target_config(self._target_config.tsv)

    @property
    def pickle(self):
        return self.target_config(self._target_config.pickle)

    @property
    def tfmodel(self):
        return self.target_config(self._target_config.tfmodel)

    @property
    def tfhistory(self):
        return self.target_config(self._target_config.tfhistory)

    @property
    def feather(self):
        return self.target_config(self._target_config.feather)

    @property
    def excel(self):
        return self.target_config(self._target_config.excel)

    @property
    def folder(self):
        """
        Input/Output path is a folder
        """
        return self.target_config(self._target_config.as_folder()).no_extension

    @property
    def folder_data(self):

        """
        Input/Output path is a folder with data (extension based on type)
        """
        return self.target_config(self._target_config.as_folder())

    def with_flag(self, flag=True):
        return self.target_config(self._target_config.with_flag(flag))

    @property
    def file_numpy(self):
        return self.target_config(self._target_config.numpy)

    @property
    def require_local_access(self):
        """
        Write parameter to local filesystem before syncing to remote filesystem
        :return: ParameterFactory
        """
        return self.target_config(self._target_config.with_require_local_access())

    def target_factory(self, target_factory, folder=False):
        tc = self.target_config(self._target_config.with_target_factory(target_factory))
        if folder:
            return tc.folder
        return tc

    def load_options(self, file_format, **options):
        current_options = self.parameter.load_options or {}
        current_options = current_options.copy()
        current_options[file_format] = options
        return self.modify(load_options=current_options)

    def save_options(self, file_format, **options):
        current_options = self.parameter.save_options or {}
        current_options = current_options.copy()
        current_options[file_format] = options
        return self.modify(save_options=current_options)

    def custom_target(self, target_factory, folder=False):
        return self.target_factory(target_factory, folder=folder)

    @property
    def no_extension(self):
        """
        output without extension
        :return: ParameterFactory
        """
        return self.modify(output_ext="")

    @property
    def no_env_interpolation(self):
        """
        Disables Environment Variables Interpolation in strings
        :return: ParameterFactory
        """
        return self.modify(env_interpolation=False)

    @property
    def disable_jinja_templating(self):
        """
        Disables Jinja template parsing
        :return: ParameterFactory
        """
        return self.modify(disable_jinja_templating=True)

    @property
    def prod_immutable(self):
        """
        Converts parameter to production immutable output
        :return: ParameterFactory
        """
        return self.modify(output_mode=OutputMode.prod_immutable)

    # kind changes
    @property
    def output(self):
        """
        Converts parameter to output
        :return: ParameterFactory
        """
        task_output = self.modify(kind=_ParameterKind.task_output)

        value_type = self.parameter.value_type
        if value_type is None:
            return task_output.target
        return task_output

    def cast(self, type_):
        return self

    def __getitem__(self, type_):
        # we want special treatment for Config classes
        if _ConfigParamContainer.is_type_config(type_):
            return self.nested_config(type_)

        value_type = get_value_type_of_type(type_, inline_value_type=True)
        if not value_type:
            raise unknown_value_type_in_parameter(type_)
        return self.modify(value_type=value_type)

    def type(self, type_):
        value_type = get_value_type_of_type(type_)
        if not value_type:
            value_type = InlineValueType(type_=type_)
        return self.modify(value_type=value_type)

    def sub_type(self, sub_type):
        return self.modify(sub_type=sub_type)

    # behaviour modifiers
    @property
    def data(self):
        """
        mark output as data (it will not be loaded during the "build" stage, while we compile all tasks graph)
        """
        if self.parameter.value_type is None:
            return self.modify(value_type=TargetValueType, load_on_build=False)
        return self.modify(load_on_build=False)

    def help(self, help_message):
        """
        A human-readable string describing the purpose of this Parameter.
        For command-line invocations, this will be used as the `help` string shown to users. Default: ``None``
        :return: ParameterFactory
        """
        return self.modify(description=help_message)

    @property
    def non_significant(self):
        """Mark parameter to not be treated as part of the unique identifier for a Task.
        An insignificant Parameter might also be used to specify a password or other
        sensitive information that should not be made public.
        :return: ParameterFactory
        """
        return self.modify(significant=False)

    def config(self, section, name):
        return self.modify(config_path=ConfigPath(section=section, key=name))

    def description(self, help_message):
        return self.modify(description=help_message)

    def default(self, value):
        return self.modify(default=value)

    def enum(self, enum, **kwargs):
        """
        A parameter whose value is an :class:`~enum.Enum`.

        In the task definition, use

        .. code-block:: python

            class Model(enum.Enum):
              Honda = 1
              Volvo = 2

            class MyTask(dbnd.Task):
              my_param = databand.EnumParameter(enum=Model)

        At the command line, use,

        .. code-block:: console

            $ dbnd MyTask --my-param Honda

        """
        return self(**kwargs).type(EnumValueType(enum))

    def choices(self, choices):
        """
            Consider using :class:`parameter.enum` for a typed, structured
            alternative.  This parameter can perform the same role when all choices are the
            same type and transparency of parameter value on the command line is
            desired.
        :param choices: An iterable, all of whose elements are of `T` to
                        restrict parameter choices to.
        :return:
        """
        return self.modify(validator=ChoiceValidator(choices))

    def numerical(self, min_value, max_value):
        return self.modify(validator=NumericalValidator(min_value, max_value))

    def custom_datetime(self, interval=1, start=None):
        custom_value_type = self.parameter.value_type.__class__(interval, start)
        return self.type(custom_value_type)

    def nested_config(self, config_cls):
        from dbnd._core.parameter.value_types.task_value import ConfigValueType

        return self(
            value_type=ConfigValueType(config_cls),
            # default=config_cls,
            significant=False,  # doesn't affect output
        )

    @property
    def c(self):
        """
        config parameter
        """
        return self.modify(empty_default=True, significant=False)

    @property
    def system(self):
        return self.modify(system=True, significant=False)

    # type aliases
    @property
    def target(self):
        return self.type(TargetValueType)

    @property
    def path_str(self):
        return self.type(TargetPathValueType)

    @property
    def pandas_dataframe(self):
        return self[pd.DataFrame]

    @property
    def data_list_str(self):
        return self[DataList[str]]

    @property
    def data_list(self):
        return self[DataList]

    @property
    def numpy_array(self):
        return self[numpy.ndarray]

    @property
    def _p(self):
        """
        inline builder for tests purposes only!
        :return:
        """
        return self.build_parameter("inline")

    def build_parameter(self, context):
        try:
            return self._build_parameter(context)
        except Exception as ex:
            raise failed_to_build_parameter(context, self.parameter, ex)

    def _build_value_type(self, context):
        s = self.parameter
        value_type = s.value_type
        default = s.default
        if value_type is None:
            if is_defined(default):
                value_type = get_value_type_of_obj(default)
                if value_type is None:
                    raise friendly_error.task_parameters.no_value_type_from_default(
                        default, context=context
                    )
            elif value_type is None:  # we don't have value type! let's fail!
                if s.load_on_build is False:  # we are in data mode
                    s.value_type = TargetValueType
                else:
                    raise friendly_error.task_parameters.no_value_type_defined_in_parameter(
                        context=context
                    )
        else:
            # let validate that what we have can be ValueType!
            resolved_value_type = get_value_type_of_type(value_type)
            if resolved_value_type is None:  # we don't have value type! let's fail!
                raise friendly_error.task_parameters.unknown_value_type_in_parameter(
                    value_type
                )
            value_type = resolved_value_type  # type: ValueType

        if s.sub_type:
            sub_value_type = get_value_type_of_type(s.sub_type)
            if isinstance(value_type, _StructureValueType):
                value_type = value_type.__class__(sub_value_type=sub_value_type)
            else:
                raise friendly_error.task_parameters.sub_type_with_non_structural_value(
                    context=context, value_type=value_type, sub_type=s.sub_type
                )
        return value_type

    def _build_parameter(self, context="inline"):
        s = self.parameter  # type: ParameterDefinition
        update_kwargs = {}

        value_type = self._build_value_type(context)

        validator = s.validator
        if s.choices:
            validator = ChoiceValidator(s.choices)

        if is_not_defined(s.default):
            if s.empty_default:
                update_kwargs["default"] = value_type._generate_empty_default()

        if not is_defined(s.load_on_build):
            update_kwargs["load_on_build"] = value_type.load_on_build

        # create value meta
        if s.value_meta_conf is None:
            update_kwargs["value_meta_conf"] = ValueMetaConf(
                log_preview=s.log_preview,
                log_preview_size=s.log_preview_size,
                log_schema=s.log_schema,
                log_size=s.log_size,
                log_stats=s.log_stats,
                log_histograms=s.log_histograms,
            )

        # Whether different values for this parameter will differentiate otherwise equal tasks
        description = s.description or ""
        if not is_defined(description):
            if s.is_output() and s.default_output_description:
                description = s.default_output_description
            elif not s.load_on_build and s.default_input_description:
                description = s.default_input_description
            else:
                description = s.default_description

            if s.validator:
                description = _add_description(description, validator.description)
            update_kwargs["description"] = description()
        # We need to keep track of this to get the order right (see Task class)
        ParameterDefinition._total_counter += 1
        if s.kind == _ParameterKind.task_output:
            update_kwargs["significant"] = False

        updated = self.modify(
            value_type=value_type,
            value_type_defined=value_type,
            validator=validator,
            description=description,
            parameter_id=ParameterDefinition._total_counter,
            **update_kwargs
        )

        return updated.parameter


PARAMETER_FACTORY = ParameterFactory()
parameter = PARAMETER_FACTORY

output = parameter.output  # type: ParameterFactory
data = parameter.target.data  # type: ParameterFactory


def build_parameter(parameter, context="inline"):
    try:
        if isinstance(parameter, ParameterFactory):
            return parameter.build_parameter(context)
        if isinstance(parameter, type) and issubclass(parameter, ParameterDefinition):
            # may be used just used name of the class
            return parameter()
    except Exception:
        logger.error("Failed to build parameter for '%s'", context)
        raise
    if not isinstance(parameter, ParameterDefinition):
        raise value_is_not_parameter_cls(parameter, context=context)

    return parameter
