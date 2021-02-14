import functools
import logging
import typing

import six

from more_itertools import last, unique_everseen
from six import iteritems

from dbnd._core.configuration.config_path import (
    CONF_CONFIG_SECTION,
    CONF_TASK_ENV_SECTION,
    CONF_TASK_SECTION,
)
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.config_store import _ConfigStore
from dbnd._core.configuration.config_value import ConfigValue, ConfigValuePriority
from dbnd._core.configuration.pprint_config import pformat_current_config
from dbnd._core.constants import RESULT_PARAM, ParamValidation, _TaskParamContainer
from dbnd._core.current import get_databand_context
from dbnd._core.decorator.task_decorator_spec import args_to_kwargs
from dbnd._core.errors import MissingParameterError, friendly_error
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    build_parameter_value,
)
from dbnd._core.parameter.parameter_value import (
    ParameterFilters,
    Parameters,
    ParameterValue,
    fold_parameter_value,
)
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    task_context,
    try_get_current_task,
)
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_passport import format_source_suffix
from dbnd._core.task_build.task_signature import (
    TASK_ID_INVALID_CHAR_REGEX,
    build_signature,
)
from dbnd._core.utils.basics.text_banner import safe_string
from targets import target
from targets.target_config import parse_target_config


if typing.TYPE_CHECKING:
    from typing import Type, Any, List, Optional
    from dbnd._core.configuration.dbnd_config import DbndConfig
    from dbnd._core.settings import EnvConfig
    from dbnd._core.task.task_with_params import _TaskWithParams

TASK_BAND_PARAMETER_NAME = "task_band"
logger = logging.getLogger(__name__)


class TaskFactoryConfig(object):
    """(Advanced) Databand's core task builder"""

    _conf__task_family = "task_build"

    def __init__(self, verbose, sign_with_full_qualified_name, sign_with_task_code):
        self.verbose = verbose
        self.sign_with_full_qualified_name = sign_with_full_qualified_name
        self.sign_with_task_code = sign_with_task_code

    @classmethod
    def from_dbnd_config(cls, conf):
        def _b(param_name):
            return conf.getboolean(cls._conf__task_family, param_name)

        return cls(
            verbose=_b("verbose"),
            sign_with_full_qualified_name=_b("sign_with_full_qualified_name"),
            sign_with_task_code=_b("sign_with_task_code"),
        )


def get_task_from_sections(config, task_name):
    extra_sections = []
    while task_name:
        # "pseudo" recursive call
        # we check if we have something  for current task_name, and if we do - that's a from
        task_from = config.get(task_name, "_from", None)
        if task_from == task_name:
            # let not throw exception, as it's not a critical error.
            task_from = None

        if task_from:
            extra_sections.append(task_from)
        task_name = task_from

    return extra_sections


class TaskFactory(object):
    """
    Create a TaskWithParam object by
    1. Calculating its params
    2. Register to cache
    3. Calling Tasks initializing methods - _initialize and _validate
    """

    def __init__(self, config, task_cls, task_definition, task_args, task_kwargs):
        # type:(DbndConfig, Type[_TaskWithParams],TaskDefinition, Any, Any)->None
        self.task_cls = task_cls
        self.task_definition = task_definition

        # keep copy of user inputs
        self.task_kwargs__ctor = task_kwargs.copy()
        self.task_args__ctor = list(task_args)

        self.parent_task = try_get_current_task()

        self._ctor_as_str = "%s@%s" % (
            _get_call_repr(
                self.task_passport.task_family,
                self.task_args__ctor,
                self.task_kwargs__ctor,
            ),
            str(self.task_cls),
        )

        # extract all "system" keywords from kwargs
        # support task_family in kwargs -> use it as task_name (old behavior)
        task_family = task_kwargs.get("task_family", self.task_passport.task_family)
        self.task_name = task_kwargs.pop("task_name", task_family)
        self.task_config_override = task_kwargs.pop("override", None) or {}
        task_config_sections_extra = task_kwargs.pop("task_config_sections", None)
        self.task_kwargs = task_kwargs

        self.task_name = TASK_ID_INVALID_CHAR_REGEX.sub("_", self.task_name)

        self.task_factory_config = TaskFactoryConfig.from_dbnd_config(config)
        self.verbose_build = self.task_factory_config.verbose

        self.config = config
        self.config_sections = []

        self.task_errors = []
        self.build_warnings = []

        # user gives explicit name, or it full_task_family
        self.task_main_config_section = (
            self.task_name or self.task_definition.task_config_section
        )
        self.config_sections = self._get_task_config_sections(
            config=config, task_config_sections_extra=task_config_sections_extra
        )
        self.task_enabled = True

    @property
    def task_passport(self):
        return self.task_definition.task_passport

    @property
    def task_family(self):
        return self.task_definition.task_passport.task_family

    def _get_config_values_stack(self, key):
        # type: (str) -> List[ConfigValue]
        # see get_multisection_config_value documentation
        return self.config.get_multisection_config_value(self.config_sections, key)

    def _get_config_value_stack_for_param(self, param_def):
        # type: (ParameterDefinition) -> List[Optional[ConfigValue]]
        """
        Wrap the extracting the value of param_def from configuration or config_path
        """
        try:
            config_values_stack = self._get_config_values_stack(key=param_def.name)
            if config_values_stack:
                return config_values_stack

            if param_def.config_path:
                return [
                    self.config.get_config_value(
                        section=param_def.config_path.section,
                        key=param_def.config_path.key,
                    )
                ]
            return [None]
        except Exception as ex:
            raise param_def.parameter_exception("read configuration value", ex)

    def _build_parameter_value(self, param_def):
        """
        This is the place we calculate param_def value
        based on Class(defaults, constructor, overrides, root)
        and Config(env, cmd line, config)  state
        """
        # getting optionally multiple config values
        config_values_stack = self._get_config_value_stack_for_param(param_def)
        parameter_values = [
            self._build_single_parameter_value(config_value, param_def)
            for config_value in config_values_stack
        ]

        # after collecting the parameter values we need to reduce them to a single value
        # if there is only one - reduce will get it
        # other wise we need combine the values using `fold_parameter_value`
        # may raise if can't fold two values together!!
        return functools.reduce(fold_parameter_value, parameter_values)

    def _build_single_parameter_value(self, param_config_value, param_def):
        # type: (Optional[ConfigValue], ParameterDefinition) -> ParameterValue
        """
        Build parameter value from config_value and param_definition.
        Considerate - priority, constructor values, param type and so on.
        """
        param_name = param_def.name

        # first check for override (HIGHEST PRIORIOTY)
        if (
            param_config_value
            and param_config_value.priority >= ConfigValuePriority.OVERRIDE
        ):
            cf_value = param_config_value

        # second using kwargs we received from the user
        # do we need to do it for tracking?
        elif param_name in self.task_kwargs:
            cf_value = ConfigValue(
                self.task_kwargs.get(param_name), source=self._source_name("ctor")
            )

        elif param_config_value:
            cf_value = param_config_value

        elif param_name in self.task_definition.param_defaults:
            # we can't "add" defaults to the current config and rely on configuration system
            # we don't know what section name to use, and it my clash with current config
            cf_value = ConfigValue(
                self.task_definition.param_defaults.get(param_name),
                source=self._source_name("default"),
            )

        else:
            cf_value = None

        if cf_value is None and not param_def.is_output():
            # outputs can be none, we "generate" their values later
            err_msg = "No value defined for '{name}' at {context_str}".format(
                name=param_name, context_str=self._ctor_as_str
            )
            raise MissingParameterError(
                err_msg,
                help_msg=param_def._get_help_message(sections=self.config_sections),
            )

        return build_parameter_value(param_def, cf_value)

    def _source_name(self, name):
        return self.task_definition.task_passport.format_source_name(name)

    # should refactored out
    def _get_task_config_sections(self, config, task_config_sections_extra):
        # there is priority of task name over task family, as name is more specific
        sections = [self.task_name]

        # _from at config files
        sections.extend(get_task_from_sections(config, self.task_name))

        # sections by family
        sections.extend(
            [self.task_passport.task_family, self.task_passport.full_task_family]
        )

        # from user
        if task_config_sections_extra:
            sections.extend(task_config_sections_extra)

        # adding "default sections"  - LOWEST PRIORITY
        if issubclass(self.task_cls, _TaskParamContainer):
            sections += [CONF_TASK_SECTION]

        from dbnd._core.task.config import Config

        if issubclass(self.task_cls, Config):
            sections += [CONF_CONFIG_SECTION]

        # dedup the values
        sections = list(unique_everseen(filter(None, sections)))

        return sections

    def _get_override_params_signature(self):
        override_signature = {}
        for p_obj, p_val in six.iteritems(self.task_config_override):
            if isinstance(p_obj, ParameterDefinition):
                override_key = "%s.%s" % (p_obj.task_definition.task_family, p_obj.name)
                override_value = (
                    p_val
                    if isinstance(p_val, six.string_types)
                    else p_obj.signature(p_val)
                )
            else:
                # very problematic approach till we fix the override structure
                override_key = str(p_obj)
                override_value = str(p_val)
            override_signature[override_key] = override_value
        return override_signature

    def build_task_object(self, task_metaclass):
        databand_context = get_databand_context()

        # convert args to kwargs, validate values
        self.task_kwargs = self._build_and_validate_task_ctor_kwargs(
            self.task_args__ctor, self.task_kwargs
        )

        self._log_build_step("Resolving task params with %s" % self.config_sections)
        try:
            task_param_values = self._build_task_param_values()
            task_params = Parameters(
                source=self._ctor_as_str, param_values=task_param_values
            )

        except Exception:
            self._log_config(force_log=True)
            raise

        if self.parent_task and not self.parent_task.ctrl.should_run():
            self.task_enabled = False

        # load from task_band if exists
        task_band_param = task_params.get_param_value(TASK_BAND_PARAMETER_NAME)
        if task_band_param and task_band_param.value:
            task_band = task_band_param.value

            # we are going to load all task parameters from task_band
            task_params = self.load_task_params_from_task_band(task_band, task_params)

        # update [task] section with Scope.children params
        if not self.task_cls._conf__no_child_params:
            self.build_and_apply_task_children_config(task_params=task_params)

        params = task_params.get_params_signatures(ParameterFilters.SIGNIFICANT_INPUTS)

        # we add override to Object Cache signature
        override_signature = self._get_override_params_signature()
        # task schema id is unique per Class definition.
        # so if we have new implementation - we will not a problem with rerunning it
        full_task_name = "%s@%s(object=%s)" % (
            self.task_name,
            self.task_definition.full_task_family,
            str(id(self.task_definition)),
        )

        # now we don't know the real signature - so we calculate signature based on all known params
        cache_object_signature = build_signature(
            name=full_task_name,
            params=params,
            extra={"task_override": override_signature},
        )
        self._log_build_step(
            "Task task_signature %s" % str(cache_object_signature.signature)
        )

        # If a Task has already been instantiated with the same parameters,
        # the previous instance is returned to reduce number of object instances.
        tic = databand_context.task_instance_cache
        cached_task_object = tic.get_cached_task_obj(cache_object_signature)
        if cached_task_object and not hasattr(cached_task_object, "_dbnd_no_cache"):
            return cached_task_object

        # we want to have task id immediately, so we can initialize outputs/use by user
        # we should switch to SIGNIFICANT_INPUT here
        task_signature_obj = build_signature(
            name=self.task_name,
            params=params,
            extra=self.task_definition.task_signature_extra,
        )

        task = task_metaclass._build_task_obj(
            task_definition=self.task_definition,
            task_name=self.task_name,
            task_params=task_params,
            task_signature_obj=task_signature_obj,
            task_config_override=self.task_config_override,
            task_config_layer=self.config.config_layer,
            task_enabled=self.task_enabled,
            task_sections=self.config_sections,
        )
        tic.register_task_obj_cache_instance(
            task, task_obj_cache_signature=cache_object_signature
        )

        task.task_call_source = [
            databand_context.user_code_detector.find_user_side_frame(2)
        ]
        if task.task_call_source and self.parent_task:
            task.task_call_source.extend(self.parent_task.task_call_source)

        # now the task is created - all nested constructors will see it as parent
        with task_context(task, TaskContextPhase.BUILD):
            task._initialize()
            task._validate()

            # it might be that config has been changed even more
            task.task_config_layer = self.config.config_layer

        # only now we know "task_id" so we can register in "publicaly facing cache
        tic.register_task_instance(task)

        return task

    def _update_params_def_target_config(self, param_def):
        """
        calculates parameter.target_config based on extra config at parameter__target value
        user might want to change the target_config for specific param using configuration
        """

        # used for target_format update
        # change param_def definition based on config state
        target_option = "%s__target" % param_def.name
        target_config = self._get_config_values_stack(key=target_option)
        if not target_config:
            return param_def

        # the last is from a higher level
        target_config = last(target_config)
        try:
            target_config = parse_target_config(target_config.value)
        except Exception as ex:
            raise param_def.parameter_exception(
                "Calculate target config for %s : target_config='%s'"
                % (target_option, target_config.value),
                ex,
            )

        param_def = param_def.modify(target_config=target_config)
        return param_def

    def _build_task_param_values(self):
        """
        This process is composed from those parts:
        1. Editing the config while building params - depend on some magic params
        2. Validating the params
        """
        # copy because we about to edit it
        params_to_build = self.task_definition.task_param_defs.copy()
        # remove all "config related" params
        param_task_env = params_to_build.pop("task_env", None)
        param_task_config = params_to_build.pop("task_config", None)
        task_param_values = []

        # let's start to apply layers on current config
        # at the end of the build we will save current layer and will make it "active" config
        # during task execution/initializaiton

        # STEP: Apply all "override" values in Task(override={})
        if self.task_config_override:
            self.config.set_values(
                source=self._source_name("ctor[override]"),
                config_values=self.task_config_override,
                priority=ConfigValuePriority.OVERRIDE,
            )

        # STEP: Task may have TaskClass.defaults= {...}
        # add these values, so task build or nested tasks/configs builds will see it
        task_defaults = self.task_definition.task_defaults_config_store
        if task_defaults:
            self.config.set_values(
                task_defaults, source=self._source_name("task.defaults")
            )

        # STEP: build and apply any Task class level config
        # User has specified "task_config = ..."
        # Only orchestration tasks has this property
        if param_task_config:
            task_param_values.append(
                self.build_and_apply_task_config(param_task_config)
            )

        # STEP: build and apply Environment level config
        if param_task_env:
            task_param_values.append(self.build_and_apply_task_env(param_task_env))

        # STEP: log with the final config
        self._log_config()

        # calculate configuration per parameter, and calculate parameter value
        for param_def in params_to_build.values():
            param_def = self._update_params_def_target_config(param_def)
            try:
                p_value = self._build_parameter_value(param_def)
                task_param_values.append(p_value)
            except MissingParameterError as ex:
                self.task_errors.append(ex)

        # validation for errors and log warnings
        self.validate_no_extra_config_params(task_param_values)

        if self.task_errors:
            self._raise_task_build_errors()

        if self.build_warnings:
            self._log_task_build_warnings()

        return task_param_values

    def validate_no_extra_config_params(self, task_param_values):
        task_param_values = {param.name: param for param in task_param_values}

        if "validate_no_extra_params" not in task_param_values:
            return

        validate_no_extra_params = task_param_values["validate_no_extra_params"].value
        if validate_no_extra_params == ParamValidation.disabled:
            return

        # Must lower task parameter name to comply to case insensitivity of configuration
        task_param_names = {
            name.lower() for name in self.task_definition.task_param_defs
        }

        for key, value in _iterate_config_items(self.config, self.config_sections):
            if (
                value.source.endswith(
                    format_source_suffix(ParameterScope.children.value)
                )
                or key in ["_type", "_from"]
                or key.endswith("__target")
            ):
                # all those are exceptions we want to ignore
                continue

            if key not in task_param_names:
                # those are keys we need to warn about
                exc = friendly_error.task_build.unknown_parameter_in_config(
                    task_name=self.task_name,
                    param_name=key,
                    source=value.source,
                    task_param_names=task_param_names,
                    config_type=self._get_task_or_config_string(),
                )

                if validate_no_extra_params == ParamValidation.warn:
                    self.build_warnings.append(exc)

                elif validate_no_extra_params == ParamValidation.error:
                    self.task_errors.append(exc)

    def build_and_apply_task_children_config(self, task_params):
        # type: (Parameters)-> None

        param_values = []
        for p_val in task_params.get_param_values():
            #  we want only parameters of the right scope -- children
            if p_val.parameter.scope == ParameterScope.children:
                param_values.append((p_val.name, p_val.value))
        if param_values:
            source = self._source_name("children")
            config_store = parse_and_build_config_store(
                source=source, config_values={CONF_TASK_SECTION: dict(param_values)},
            )
            update_config_section_on_change_only(
                self.config,
                config_store,
                source=self._source_name(ParameterScope.children.value),
                on_change_only=True,
            )

    def _build_and_validate_task_ctor_kwargs(self, task_args, task_kwargs):
        """
        validate is required to handle with parameters with no definition

        this can be caused by function declaration:
        >>> def func(element, *args, k=None, **kwargs): ...

        this cal will create undefined handle for the args
         >>> func(1, problem_1,problem_2 ...)

        this cal will create undefined handle for the kwargs
        >>> func(1, s=value)
        """
        param_names = set(self.task_definition.task_param_defs)
        has_varargs = False
        has_varkwargs = False
        if self.task_cls._conf__decorator_spec is not None:
            # only in functions we can have args as we know exact "call" signature
            task_args, task_kwargs = args_to_kwargs(
                self.task_cls._conf__decorator_spec.args, task_args, task_kwargs
            )
            has_varargs = self.task_cls._conf__decorator_spec.varargs
            has_varkwargs = self.task_cls._conf__decorator_spec.varkw

        if task_args and not has_varargs:
            # we should not have any args, so we don't know how to assign them
            raise friendly_error.unknown_args_in_task_call(
                self.parent_task,
                self.task_cls,
                self.task_args__ctor,
                call_repr=self._ctor_as_str,
            )

        if not has_varkwargs:
            # there are no kwargs for the callable
            # we need to be sure that all the kwargs are for parameters
            for key in task_kwargs:
                if key not in param_names:
                    raise friendly_error.task_build.unknown_parameter_in_constructor(
                        constructor=self._ctor_as_str,
                        param_name=key,
                        task_parent=self.parent_task,
                    )

        return task_kwargs

    def build_and_apply_task_config(self, param_task_config):
        """
        calculate any task class level params and updates the config with thier values
        @pipeline(task_config={CoreConfig.tracker: ["console"])
        <or>
        class a(Task):
            task_config = {"core": {"tracker": ["console"]}}
            <or>
            task_config = {CoreConfig.tracker: ["console"]}
        """
        # calculate the value of `Task.task_config` using it's definition
        # (check for all inheritance and find value for `task_config`
        param_task_config_value = self._build_parameter_value(param_task_config)
        if param_task_config_value.value:
            # Support two modes:
            # 1. Task.param_name:333
            # 2. {"section": {"key":"value"}}
            # dict parameter value can't have non string as a key
            param_task_config_value.value = parse_and_build_config_store(
                config_values=param_task_config_value.value,
                source=self._source_name("task_config"),
            )
            # merging `Task.task_config` into current configuration
            # we are adding "ultimate" layer on top of all layers
            self.config.set_values(param_task_config_value.value)

        return param_task_config_value

    # we apply them to config only if there are no values (this is defaults)
    def build_and_apply_task_env(self, param_task_env):
        """
        find same parameters in the current task class and EnvConfig
        and take the value of that params from environment
        for example  spark_config in SparkTask  (defined by EnvConfig.spark_config)
        we take values using names only
        """
        value_task_env = self._build_parameter_value(param_task_env)
        env_config = value_task_env.value  # type: EnvConfig

        param_values = []
        #  we want only parameters of the right scope -- children
        for param_def, param_value in env_config._params.get_params_with_value(
            param_filter=ParameterFilters.CHILDREN
        ):
            param_values.append((param_def.name, param_value))
        source = self._source_name("env[%s]" % env_config.task_name)
        config_store = parse_and_build_config_store(
            source=source, config_values={CONF_TASK_ENV_SECTION: dict(param_values)},
        )
        update_config_section_on_change_only(
            self.config, config_store, source=source,
        )
        return value_task_env

    def __str__(self):
        if self.task_name == self.task_passport.task_family:
            return "TaskFactory(%s)" % self.task_name
        return "TaskFactory(%s@%s)" % (self.task_name, self.task_family)

    def load_task_params_from_task_band(self, task_band, task_params):
        task_band_value = target(task_band).as_object.read_json()

        new_params = []
        found = []
        source = "task_band.json"
        for p_value in task_params.get_param_values():
            name = p_value.name
            if name not in task_band_value or name == RESULT_PARAM:
                new_params.append(p_value)
                continue

            value = p_value.parameter.calc_init_value(task_band_value[name])
            found.append(name)
            new_parameter_value = ParameterValue(
                parameter=p_value.parameter,
                source=source,
                source_value=value,
                value=value,
            )
            new_params.append(new_parameter_value)

        logger.info(
            "Loading task '{task_family}' from {task_band}:\n"
            "\tfields taken:\t{found}".format(
                task_family=self.task_passport.task_family,
                task_band=task_band,
                found=",".join(found),
            )
        )
        return Parameters("task_band", new_params)

    def _log_build_step(self, msg, force_log=False):
        if self.verbose_build or force_log:
            logger.info("[%s] %s", self.task_name, msg)

    def _log_config(self, force_log=False):
        msg = "config for sections({config_sections}): {config}".format(
            config=pformat_current_config(
                self.config, sections=self.config_sections, as_table=True
            ),
            config_sections=self.config_sections,
        )
        self._log_build_step(msg, force_log=force_log)

    def _raise_task_build_errors(self):
        if len(self.task_errors) == 1:
            raise self.task_errors[0]

        raise friendly_error.failed_to_create_task(
            self._ctor_as_str, nested_exceptions=self.task_errors
        )

    def _get_task_or_config_string(self):
        from dbnd._core.task.config import Config

        if issubclass(self.task_cls, Config):
            return "config"
        else:
            return "task"

    def _log_task_build_warnings(self):
        w = "Build warnings for %s '%s': " % (
            self._get_task_or_config_string(),
            self.task_name,
        )

        for warning in self.build_warnings:
            w += "\n\t" + str(warning)
            if hasattr(warning, "did_you_mean") and warning.did_you_mean:
                w += "\n\t\t - " + warning.did_you_mean

        logger.warning(w)


def update_config_section_on_change_only(
    config, config_store, source, on_change_only=False
):
    if on_change_only:
        # we take values using names only
        relevant_config_store = _ConfigStore()
        for section, section_values in six.iteritems(config_store):
            for key, value in six.iteritems(section_values):
                previous_value = config.get_config_value(section, key)
                if (
                    previous_value
                    and on_change_only
                    and previous_value.value == value.value
                ):
                    continue
                relevant_config_store.set_config_value(section, key, value)
        config_store = relevant_config_store

    # we apply set on change only in the for loop, so we can optimize and not run all these code
    if config_store:
        config.set_values(config_values=config_store, source=source)


def _iterate_config_items(config, sections):
    for section_name in sections:
        section = config.config_layer.config.get(section_name)
        if section:
            for key, value in iteritems(section):
                yield key, value


def _get_call_repr(call_name, call_args, call_kwargs):
    params = ""
    if call_args:
        params = ", ".join((safe_string(repr(p), 300)) for p in call_args)
    if call_kwargs:
        if params:
            params += ", "
        params += ", ".join(
            (
                "%s=%s" % (p, safe_string(repr(k), 300))
                for p, k in iteritems(call_kwargs)
            )
        )
    return "{call_name}({params})".format(call_name=call_name, params=params)
