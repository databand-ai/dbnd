import enum
import logging
import typing

from typing import Any, Dict, Optional

import attr
import six

from dbnd._core.configuration.config_path import ConfigPath
from dbnd._core.configuration.config_value import ConfigValue
from dbnd._core.configuration.environ_config import PARAM_ENV_TEMPLATE
from dbnd._core.constants import (
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    OutputMode,
    _TaskParamContainer,
)
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors import DatabandBuildError, friendly_error
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.basics.text_banner import safe_string
from dbnd._core.utils.traversing import traverse
from dbnd._vendor.snippets.airflow_configuration import expand_env_var
from targets.base_target import Target, TargetSource
from targets.data_target import DataTarget
from targets.file_target import FileTarget
from targets.inline_target import InlineTarget
from targets.inmemory_target import InMemoryTarget
from targets.target_config import FileFormat, TargetConfig
from targets.target_factory import target
from targets.types import Path
from targets.value_meta import ValueMetaConf
from targets.values import (
    InlineValueType,
    StrValueType,
    ValueType,
    VersionValueType,
    get_types_registry,
)
from targets.values.builtins_values import DefaultObjectValueType
from targets.values.structure import _StructureValueType
from targets.values.target_values import _TargetValueType


logger = logging.getLogger(__name__)
if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task
    from dbnd._core.task_build.task_definition import TaskDefinition
    from dbnd._core.parameter import ParameterValue


class _ParameterKind(enum.Enum):
    task_input = "task_input"
    task_output = "task_output"


class ParameterGroup(enum.Enum):
    system = "system"
    user = "user"


T = typing.TypeVar("T")


@attr.s(repr=False, hash=False, str=False)
class ParameterDefinition(object):  # generics are broken: typing.Generic[T]
    """
    Parameter whose value is a ``str``, and a base class for other parameter types.

    Parameters are objects set on the Task class level to make it possible to parameterize tasks.
    For instance:

    .. code:: python

        class MyTask(dbnd.Task):
            foo = databand.parameter[str]

        class RequiringTask(dbnd.Task):
            def requires(self):
                return MyTask(foo="hello")

            def run(self):
                print(self.requires().foo)  # prints "hello"

    This makes it possible to instantiate multiple tasks, eg ``MyTask(foo='bar')`` and
    ``MyTask(foo='baz')``. The task will then have the ``foo`` attribute set appropriately.

    When a task is instantiated, it will first use any argument as the value of the parameter, eg.
    if you instantiate ``a = TaskA(x=44)`` then ``a.x == 44``. When the value is not provided, the
    value  will be resolved in this order of falling priority:

        * Any value provided on the command line:

          - To the root task (eg. ``--param xyz``)

          - Then to the class, using the qualified task name syntax (eg. ``--TaskA-param xyz``).

        * With ``[TASK_NAME]>PARAM_NAME: <serialized value>`` syntax. See :ref:`ParamConfigIngestion`

        * Any default value set using the ``default`` flag.

    """

    default_description = ""
    default_input_description = "data input"
    default_output_description = "data output"

    _total_counter = (
        0  # non-atomically increasing counter used for ordering parameters.
    )

    default = attr.ib(default=NOTHING)

    name = attr.ib(default=None)

    # value type and sub type
    value_type = attr.ib(default=None)  # type: ValueType
    value_type_defined = attr.ib(default=None)  # type: ValueType
    sub_type = attr.ib(default=None)

    description = attr.ib(default=NOTHING)  # type: str
    config_path = attr.ib(default=None)  # type: Optional[ConfigPath]
    disable_jinja_templating = attr.ib(default=False)  # type: bool
    require_local_access = attr.ib(default=False)  # type: bool
    env_interpolation = attr.ib(default=True)
    # parameter kind
    significant = attr.ib(default=True)  # type: bool
    scope = attr.ib(default=ParameterScope.task)
    from_task_env_config = attr.ib(default=False)
    system = attr.ib(default=False)
    kind = attr.ib(default=_ParameterKind.task_input)

    # output configuration
    output_name = attr.ib(default=None)  # type: str
    output_ext = attr.ib(default=None)  # type: str
    output_mode = attr.ib(default=OutputMode.regular)
    # used for output factories only
    output_factory = attr.ib(default=None)

    target_config = attr.ib(default=TargetConfig())
    load_options = attr.ib(factory=dict)  # type: Dict[FileFormat, Dict[str, Any]]
    save_options = attr.ib(factory=dict)  # type: Dict[FileFormat, Dict[str, Any]]

    validator = attr.ib(default=None)
    choices = attr.ib(default=None)

    load_on_build = attr.ib(default=NOTHING)  # type: bool
    empty_default = attr.ib(default=NOTHING)

    # value preview and meta settings
    log_preview = attr.ib(default=None)  # type: Optional[bool]
    log_preview_size = attr.ib(default=None)  # type: Optional[int]
    log_schema = attr.ib(default=None)  # type: Optional[bool]
    log_size = attr.ib(default=None)  # type: Optional[bool]
    log_stats = attr.ib(default=None)  # type: Optional[bool]
    log_histograms = attr.ib(default=None)  # type: Optional[bool]

    log_meta = attr.ib(
        default=True
    )  # type: bool  # log all (can disable whole value log)

    # ParameterDefinition ownership
    task_definition = attr.ib(default=None)  # type: TaskDefinition
    parameter_origin = attr.ib(default=None)
    parameter_id = attr.ib(default=1)

    value_meta_conf = attr.ib(default=None)  # type: ValueMetaConf
    hidden = attr.ib(default=False)  # type: bool

    @property
    def group(self):
        return ParameterGroup.system if self.system else ParameterGroup.user

    @property
    def task_definition_uid(self):
        if not self.task_definition:
            return None
        return self.task_definition.task_definition_uid

    def evolve_with_owner(self, task_definition, name):
        if self.task_definition and self.name != name:
            logger.warning(
                "Name of parameter has been changed from '%s' to '%s' at %s",
                name,
                self.name,
                task_definition,
            )
        parameter_origin = self.parameter_origin or task_definition
        return attr.evolve(
            self,
            task_definition=task_definition,
            name=name,
            parameter_origin=parameter_origin,
        )

    def parse_from_str(self, x):  # type: (str) -> T
        """
        Parse an individual value from the input.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return self.calc_init_value(x)

    def calc_init_value(self, value):
        if value is None:
            # it's None
            # TODO: may be we still can "normalize" the value
            return value
        if isinstance(value, Path):
            return target(str(value), config=self.target_config)
        if isinstance(value, Target):
            # it's deferred result - > we load it lately
            return value

        # we process value regardless parse!
        # cf_value.require_parse:

        if self.env_interpolation and isinstance(value, six.string_types):
            try:
                value = expand_env_var(value)
            except Exception as ex:
                logger.warning(
                    "failed to expand variable '%s' : %s", safe_string(value), str(ex)
                )

        # in case we are output and have value:
        # it's Target or it's str to be converted as target
        load_value = self.load_on_build and not self.is_output()

        return self.value_type.parse_value(
            value, load_value=load_value, target_config=self.target_config
        )

    def calc_runtime_value(self, value, task):
        if value is None:
            return value

        if isinstance(self.value_type, _TargetValueType):
            # if it "target" type, let read it into "user friendly" format
            # regardless it's input or output,
            # so if function has  param = output[Path] - it will get Path

            return traverse(value, self.value_type.target_to_value)

        # usually we should not load "outputs" on read
        if self.is_output():
            # actually we should not load it, so just return
            return value

        if isinstance(value, Target):
            try:
                runtime_value = self.load_from_target(value)
                if self.is_input():
                    self._log_parameter_value(runtime_value, value, task)
                return runtime_value
            except Exception as ex:
                raise friendly_error.failed_to_read_target_as_task_input(
                    ex=ex, task=task, parameter=self, target=value
                )

        if (
            isinstance(self.value_type, _StructureValueType)
            and self.value_type.sub_value_type
        ):
            try:

                def load_with_preview(val):
                    runtime_val = self.value_type.sub_value_type.load_runtime(val)
                    if self.is_input() and isinstance(val, Target):
                        # Optimisation opportunity: log all targets in a single call
                        self._log_parameter_value(runtime_val, val, task)

                    return runtime_val

                return traverse(value, convert_f=load_with_preview)
            except Exception as ex:
                raise friendly_error.failed_to_read_task_input(
                    ex=ex, task=task, parameter=self, target=value
                )

        return value

    def to_str(self, x):  # type: (T) -> str
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        if isinstance(x, Target):
            return str(x)

        return self.value_type.to_str(x)  # default impl

    def to_repr(self, x):  # type: (T) -> str
        return self.value_type.to_repr(x)

    def signature(self, x):
        if x is None:
            return str(x)

        if isinstance(x, Target):
            return str(x)

        # we can have
        # 1. a value of value_type
        # 2. target with value type TargetValueType
        # 3. list/dict of targets with value type TargetValueType
        return self.value_type.to_signature(x)

    def load_from_target(
        self, target, **kwargs
    ):  # type: (ParameterDefinition, FileTarget, **Any)-> T
        from targets import InMemoryTarget

        if isinstance(target, InMemoryTarget):
            value = target.load()
        else:
            if target.config:
                f = target.config.format
                if f and f in self.load_options:
                    kwargs.update(**self.load_options[f])
            value = self.value_type.load_from_target(target, **kwargs)

        self.validate(value)

        self._store_value_origin_target(value, target)

        return value

    def dump_to_target(
        self, target, value, **kwargs
    ):  # type: (DataTarget, T, **Any)-> None
        if hasattr(target, "config"):
            f = target.config.format
            if f and f in self.save_options:
                kwargs.update(**self.save_options[f])

        self.value_type.save_to_target(target, value, **kwargs)  # default impl

        # we need updated target
        self._store_value_origin_target(value, target)

    def _log_parameter_value(self, runtime_value, value, task):
        if try_get_databand_run() and task.current_task_run:
            task.current_task_run.tracker.log_parameter_data(
                parameter=self,
                target=value,
                value=runtime_value,
                operation_type=DbndTargetOperationType.read,
                operation_status=DbndTargetOperationStatus.OK,
            )

    def _store_value_origin_target(self, value, target):
        dbnd_run = try_get_databand_run()
        if not dbnd_run:
            return

        dbnd_run.target_origin.add(target, value, self.value_type)

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
        if isinstance(self.value_type, _TargetValueType):
            # can not move to value_type, we need target_config

            from dbnd._core.utils.task_utils import to_targets

            return to_targets(x, from_string_kwargs=dict(config=self.target_config))
        return self.value_type.normalize(x)

    def validate(self, x):
        if self.validator:
            self.validator.validate(self, x)

    def as_str_input(self, value):
        if value is None:
            return "@none"

        switch_value = self.to_str(value)
        if isinstance(value, Target):
            if self.load_on_build:
                # this is non-data parameter, it's int/str/bool
                # we are in the scenario, when something should be loaded, however, it's still Target
                switch_value = "@target:%s" % switch_value
        return switch_value

    def next_in_enumeration(self, value):
        """
        If your Parameter type has an enumerable ordering of values. You can
        choose to override this method. This method is used by the
        :py:mod:`databand.execution_summary` module for pretty printing
        purposes. Enabling it to pretty print tasks like ``MyTask(num=1),
        MyTask(num=2), MyTask(num=3)`` to ``MyTask(num=1..3)``.

        :param value: The value
        :return: The next value, like "value + 1". Or ``None`` if there's no enumerable ordering.
        """
        return self.value_type.next_in_enumeration(value)  # default impl

    def _get_help_message(self, sections=None):
        sections = sections or [(self.task_family)]

        define_via = []

        define_via.append(
            "project.cfg : [%s]%s=VALUE" % (" | ".join(sections), self.name)
        )
        define_via.append("cli:   --set %s.%s=VALUE" % (self.task_family, self.name))
        define_via.append(
            "constructor: %s(%s=VALUE, ...)" % (self.task_family, self.name)
        )
        define_via = "\n".join(["\t* %s" % l for l in define_via])

        return "You can change '{task_family}.{name}' value using one of the following methods: \n {methods}".format(
            task_family=(self.task_family), name=self.name, methods=define_via
        )

    def parameter_exception(self, reason, ex):
        err_msg = "Failed to {reason} for parameter '{name}' at {task_family}()".format(
            reason=reason, name=self.name, task_family=self.task_family
        )
        log_exception(err_msg, ex, logger)
        raise DatabandBuildError(
            err_msg, nested_exceptions=[ex], help_msg=self._get_help_message()
        )

    @property
    def task_family(self):
        if self.task_definition:
            return self.task_definition.task_family
        return None

    @property
    def task_config_section(self):
        if self.task_definition:
            return self.task_definition.task_config_section
        return None

    def __repr__(self):
        owned_by = ""
        parameter_origin = ""  # show it only if different

        if self.task_definition:
            owned_by = self.task_definition.task_family if self.task_definition else ""
            origin_cls_str = (
                self.parameter_origin.task_family if self.parameter_origin else ""
            )
            if origin_cls_str and origin_cls_str != owned_by:
                parameter_origin = " at %s" % origin_cls_str

        parameter_kind = (
            "output" if self.kind == _ParameterKind.task_output else "parameter"
        )
        return "{owned_by}.{name}({parameter_kind}[{value_type}]{parameter_origin})".format(
            owned_by=owned_by,
            parameter_origin=parameter_origin,
            value_type=self.value_type_str,
            parameter_kind=parameter_kind,
            name=self.name or "_unknown_",
        )

    @property
    def value_type_str(self):
        if self.value_type is None:
            return "unknown"
        type_handler = self.value_type.type_str
        if isinstance(self.value_type, InlineValueType):
            type_handler = "!" + type_handler
        if self.value_type_defined != self.value_type:
            type_handler = "*" + type_handler
        return type_handler

    def _target_source(self, task):
        return TargetSource(
            task_id=task.task_id, parameter_name=self.name, name=self.name
        )

    def build_target(self, task):  # type: (ParameterDefinition, Task) -> DataTarget
        target_config = self.target_config
        if not target_config.format:
            default_config = task.settings.output.get_value_target_config(
                self.value_type
            )
            # for now we take only format and compression from config
            target_config = target_config.with_format(
                default_config.format
            ).with_compression(default_config.compression)
        output_ext = self.output_ext
        if output_ext is None:
            output_ext = target_config.get_ext()
        return task.get_target(
            name=self.output_name or self.name,
            output_ext=output_ext,
            config=target_config,
            output_mode=self.output_mode,
        )

    def build_output(self, task):
        if self.output_factory is not None:
            try:
                return self.output_factory(task, self)
            except Exception:
                logger.exception(
                    "Failed to created task output %s for %s : "
                    " output_factory expected signature is '(Task, Parameter) -> Target(any structure) '",
                    self,
                    task,
                )
                raise

        if (
            not self.system
            and self.name not in ("task_band",)
            and task.task_in_memory_outputs
        ):
            return InMemoryTarget(
                path="memory://{value_type}:{task}.{p_name}".format(
                    value_type=self.value_type, task=task.task_id, p_name=self.name
                )
            )

        # we separate into two functions ,
        # as we want to be able to call build_target from output_factory implementation
        try:
            return self.build_target(task)
        except Exception as e:
            raise friendly_error.task_build.failed_to_build_output_target(
                self.name, task, e
            )

    def is_input(self):
        return self.kind == _ParameterKind.task_input

    def is_output(self):
        return self.kind == _ParameterKind.task_output

    def __hash__(self):
        return hash(self.name) ^ hash(self.task_definition)

    def modify(self, **kwargs):
        if not kwargs:
            return self
        return attr.evolve(self, **kwargs)

    def get_env_key(self, section):
        return PARAM_ENV_TEMPLATE.format(S=section.upper(), K=self.name.upper())

    def get_value_meta(self, value, meta_conf):
        # do not use meta_conf directly, you should get it merged with main config
        return self.value_type.get_value_meta(value, meta_conf=meta_conf)


def _update_parameter_from_runtime_value_type(parameter, value):
    # type: ( ParameterDefinition, Any)->Optional[ValueType]

    original_value_type = parameter.value_type
    runtime_value_type = None
    if isinstance(value, Target):
        if isinstance(value, InlineTarget):
            runtime_value_type = value.value_type
        elif isinstance(original_value_type, _TargetValueType):
            # user expects to get target/path/str
            # we will validate that value is good at "parse"
            pass
        else:
            # we are going to "load" the value from target into original_value_type
            # let get the "real" value type from the source of it
            if value.source and value.source_parameter:
                # we can take value type from the creator of it
                runtime_value_type = value.source_parameter.value_type
                if isinstance(
                    runtime_value_type, (_TargetValueType, DefaultObjectValueType)
                ):
                    return None
            else:
                # we don't really have value type
                return None
    else:
        if isinstance(value, six.string_types) or isinstance(value, Path):
            # str are going to be parsed, or treated as Target
            # Path is going to be used as Target
            return None

        runtime_value_type = get_types_registry().get_value_type_of_obj(value)

    # not found or the same
    if not runtime_value_type or runtime_value_type == original_value_type:
        return None

    # value is str, most chances we will parse it into the value
    if type(runtime_value_type) in [StrValueType, VersionValueType]:
        return None
    if isinstance(runtime_value_type, _StructureValueType):
        if isinstance(original_value_type, _StructureValueType):
            if type(runtime_value_type) == type(original_value_type):
                # probably we have difference on sub level,
                # there is no a clear way to find sub types of object
                if not runtime_value_type.sub_value_type:
                    return None
    if original_value_type.type is object:
        if runtime_value_type.type is object:
            return None
        if not isinstance(original_value_type, DefaultObjectValueType):
            return None
    return runtime_value_type


def build_parameter_value(parameter, cf_value):
    # type: (ParameterDefinition, ConfigValue) -> ParameterValue

    from dbnd._core.parameter.parameter_value import ParameterValue

    warnings = []
    value = cf_value.value
    try:
        if value is not None and not parameter.is_output():
            updated_value_type = _update_parameter_from_runtime_value_type(
                parameter, value
            )
            message = (
                "{parameter}: type of the value at runtime '{runtime}'"
                " doesn't match user defined type '{compile}'".format(
                    parameter=parameter,
                    runtime=updated_value_type,
                    compile=parameter.value_type,
                )
            )
            if updated_value_type:
                if isinstance(parameter.value_type, DefaultObjectValueType):
                    # we are going to update
                    parameter = attr.evolve(
                        parameter,
                        value_type=updated_value_type,
                        load_on_build=updated_value_type.load_on_build,
                    )
                    message = "%s: updating parameter with the runtime info" % (message)
                # warn anyway
                warnings.append(message)

    except Exception as ex:
        # we don't want to fail user code on failed value discovery
        # we only print message from "friendly exception" and show real stack
        logger.exception("Failed to discover runtime for %s" % parameter)

    try:
        p_val = parameter.calc_init_value(value)
    except Exception as ex:
        raise parameter.parameter_exception(
            "calculate value from '%s'" % safe_string(value, 100), ex=ex
        )

    # we need to break strong reference between tasks
    # otherwise we will have pointer from task to another task
    # if p_val is task, that's ok, but let minimize the risk by patching cf_value
    if isinstance(value, _TaskParamContainer):
        cf_value.value = str(cf_value)

    try:
        if p_val is not None and not isinstance(p_val, Target):
            parameter.validate(p_val)
    except Exception as ex:
        raise parameter.parameter_exception(
            "validate value='%s'" % safe_string(p_val), ex=ex
        )

    p_value = ParameterValue(
        parameter=parameter,
        source=cf_value.source,
        source_value=cf_value.value,
        value=p_val,
        parsed=cf_value.require_parse,
        warnings=warnings + cf_value.warnings,
    )

    return p_value


def infer_parameter_value_type(parameter, value):
    warnings = []
    runtime_value_type = get_types_registry().get_value_type_of_obj(value)
    if not runtime_value_type:
        runtime_value_type = DefaultObjectValueType()
    if runtime_value_type != parameter.value_type:
        if not isinstance(parameter.value_type, DefaultObjectValueType):
            message = (
                "{parameter}: type of the value at runtime '{runtime}'"
                " doesn't match user defined type '{compile}'".format(
                    parameter=parameter,
                    runtime=runtime_value_type,
                    compile=parameter.value_type,
                )
            )
            warnings.append(message)

        parameter = attr.evolve(
            parameter,
            value_type=runtime_value_type,
            value_type_defined=parameter.value_type,
            load_on_build=runtime_value_type.load_on_build,
        )
    return parameter, warnings


def _add_description(base, extra):
    base = base or ""

    if base:
        base += " "
    base += extra
