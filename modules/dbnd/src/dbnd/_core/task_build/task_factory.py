# © Copyright Databand.ai, an IBM Company 2022

"""
Task build process
=================
TaskFactory is the class that is responsible for Task/Config object creation

Class.param is calculated with following rules (top to low priority):
 1. Config value with Override priority
 2. ctor(param=VALUE)             # regular ctor
 3. Config value
 4. Parent task parameter with the same name and defined as paramter(scope=ParameterScope.children)
 5. if paramter(task_from_env=True), VALUE := Task.task_env.parameter
 6. parameter(default=VALUE)

We calculate task_env and task_config first as other parameters can be change based on their value
Following config layers are added on top of existing config of configuration, environment,
 cli and other parent tasks:
     Task.defaults  with priority FALLBACK
     Task.task_config or ctor(task_config=..)
     override_config from ctor(override=override_config) with priority OVERRIDE

Config Value are calculated based on following sections (top to low priority)
  * task_name
  * task_family
  * full_task_family
  * _from=SECTION pragma at configuration
  * task for Task, config for Config

TLDR:
    Task.defaults
    -------------

    Task.defaults is the lowest level of config (DEFAULTS)
     * It's a property of the Task class. All values are merged when TaskB inherits from TaskA (at TaskDefinition)
     * task.defaults value is calculated at TaskDefinition during the "compilation of the Task class definition"
    Task.defaults value is merged into Current config (with FALLBACK priority)
     * do not support override()!!!
     * there is no "merge" behavior for values. (Dictionaries are going to be replaced)
         class TaskA(...):
            defaults = {SparkConf.conf : { 1:1} , SparkConf.jar : "taskajar }

         class TaskB(TaskA):
            defaults = {SparkConf.conf : { 1:2}  }
        TaskB.defaults == {SparkConf.conf : { 1:2} , SparkConf.jar : "taskajar }

    Task.task_config
    ----------------

    Task.task_config - extra config level
     * it "replaces" inherited task_config from super class if exists
     * it is calculated every time we create Task
     * supports override():  `task_config = {SparkConf.jar: override("my_jar")}`
     * Usually, it's used with some basic "nested" config param: like SparkConfig.conf.
     Task.task_config issues:
       1.     task_config = {SparkConf.jar: "my_jar"}
             [spark_remote]
             jar=from_config
        User will get my_jar when engine is [spark] and  "from_config" if engine is [spark_remote].
        The reason is that engine looks for the value in most relevant section which is [spark_remote],
        while SparkConf.jar provides a value for [spark] section
        The only way to workaround it right now is to use:
            task_config = {SparkConf.jar: override("my_jar")}
        The system will look for override values and if it exists,
        the override will be taken (despite best section match).

    Task(overrides=conf_value...)
    -----------------------------
     * uses the same system as task_config, but override is applied to all values


    param = parameter(scope=ParameterScope.children)
    ------------------------------------------------

    Task Parameters Child Scoping - if task defined at
    Task can get the value for some params from Parent task (parent of any level)

    for example:
      class ChildTask(Task):
          my_p = parameter() # it doesn't have to be scoped
      class ParentTask(Task):
          my_p = parameter(scope=ParameterScope.children)
        def band:
          my_p_task = ChildTask() # no explicit "wiring" of my_p



    param = parameter(task_from_env=True)
    ------------------------------------------------

    Task Parameters value default will be taken from Task.task_env[EnvConfig] object


"""
import functools
import logging
import typing

import six

from more_itertools import last, unique_everseen
from six import iteritems

from dbnd._core.configuration.config_path import CONF_CONFIG_SECTION, CONF_TASK_SECTION
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.config_store import _ConfigStore
from dbnd._core.configuration.config_value import ConfigValue, ConfigValuePriority
from dbnd._core.configuration.pprint_config import pformat_current_config
from dbnd._core.constants import RESULT_PARAM, ParamValidation, _TaskParamContainer
from dbnd._core.current import get_databand_context
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
from dbnd._core.task_build.task_passport import format_source_suffix
from dbnd._core.task_build.task_signature import (
    TASK_ID_INVALID_CHAR_REGEX,
    build_signature,
)
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.basics.text_banner import safe_string
from dbnd._core.utils.callable_spec import args_to_kwargs
from targets import target
from targets.target_config import parse_target_config


if typing.TYPE_CHECKING:
    from typing import Any, Dict, List, Optional, Type

    from dbnd._core.configuration.dbnd_config import DbndConfig
    from dbnd._core.settings import EnvConfig
    from dbnd._core.task.task_with_params import _TaskWithParams
    from dbnd._core.task_build.task_definition import TaskDefinition

TASK_BAND_PARAMETER_NAME = "task_band"
logger = logging.getLogger(__name__)


class TaskFactoryConfig(object):
    """(Advanced) Databand's core task builder"""

    config_section = "task_build"

    def __init__(self, verbose, sign_with_full_qualified_name, sign_with_task_code):
        self.verbose = verbose
        self.sign_with_full_qualified_name = sign_with_full_qualified_name
        self.sign_with_task_code = sign_with_task_code

    @classmethod
    def from_dbnd_config(cls, conf):
        # we can not use standard "Config" class creation here, as we are used for that

        def _b(param_name):
            return conf.getboolean(cls.config_section, param_name)

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

        self.task_env_config = None  # type: Optional[EnvConfig]

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

        # current config, NOTE: it's Singleton
        self.config = config
        self.config_sections = []

        self.task_errors = []
        self.build_warnings = []

        # will be used for ConfigValue
        self.config_sections = self._get_task_config_sections(
            config=config, task_config_sections_extra=task_config_sections_extra
        )

    @property
    def task_passport(self):
        return self.task_definition.task_passport

    def _get_config_value_stack_for_param(self, param_def):
        # type: (ParameterDefinition) -> List[Optional[ConfigValue]]
        """
        Wrap the extracting the value of param_def from configuration or config_path
        """
        try:
            config_values_stack = self.config.get_multisection_config_value(
                self.config_sections, key=param_def.name
            )
            if config_values_stack:
                return config_values_stack

            if param_def.config_path:
                config_path = self.config.get_config_value(
                    section=param_def.config_path.section, key=param_def.config_path.key
                )
                if config_path:
                    return [config_path]
            return []
        except Exception as ex:
            raise param_def.parameter_exception("read configuration value", ex)

    def _build_parameter_value(self, param_def):
        # type: (ParameterDefinition) -> ParameterValue
        """
               -= MAIN FUNCTION for Parameter Calculation =-
        This is the place we calculate param_def value
        based on Class(defaults, constructor, overrides, root)
        and Config(env, cmd line, config)  state

        Build parameter value from config_value and param_definition.
        Considerate - priority, constructor values, param type and so on.
        """
        config_values_stack = self._get_config_value_stack_for_param(param_def)
        param_name = param_def.name

        # if we will use config values we will need to reduce them to a single value
        # if there is only one - reduce will get it
        # other wise we need combine the values using `fold_parameter_value`
        # may raise if can't fold two values together!!

        has_overrides_in_config = any(
            cf
            for cf in config_values_stack
            if cf and cf.priority >= ConfigValuePriority.OVERRIDE
        )

        # first check for override (HIGHEST PRIORITY)
        if has_overrides_in_config:
            config_values_as_param_values = [
                build_parameter_value(param_def, cf) for cf in config_values_stack
            ]
            return functools.reduce(fold_parameter_value, config_values_as_param_values)

        # second using kwargs we received from the user
        # do we need to do it for tracking?
        elif param_name in self.task_kwargs:
            return build_parameter_value(
                param_def,
                ConfigValue(
                    self.task_kwargs.get(param_name), source=self._source_name("ctor")
                ),
            )

        if config_values_stack:
            config_values_as_param_values = [
                build_parameter_value(param_def, cf) for cf in config_values_stack
            ]
            return functools.reduce(fold_parameter_value, config_values_as_param_values)

        if (
            self.parent_task
            and param_name in self.parent_task.task_children_scope_params
        ):
            # we have parent task with param = parameter(scope=ParameterScope.children)
            # the priority is lower than config (as this one is more like "default" than "explicit config")
            # see _calculate_task_children_scope_params implementation and ParameterScope.children
            parameter_value = self.parent_task.task_children_scope_params[param_name]
            return build_parameter_value(
                param_def,
                ConfigValue(value=parameter_value.value, source=parameter_value.source),
            )

        if param_def.from_task_env_config:
            # param = parameter(from_task_env_config=True)
            # we check task.task_env.param for the value
            if not self.task_env_config:
                raise friendly_error.task_parameters.task_env_param_with_no_env(
                    context=self._ctor_as_str, key=param_name
                )
            param_env_config_value = self.task_env_config.task_params.get_param_value(
                param_name
            )
            if param_env_config_value is None:
                raise friendly_error.task_parameters.task_env_param_not_exists_in_env(
                    context=self._ctor_as_str,
                    key=param_name,
                    env_config=self.task_env_config,
                )
            return build_parameter_value(
                param_def,
                ConfigValue(
                    value=param_env_config_value.value,
                    source=param_env_config_value.source,
                ),
            )

        if param_name in self.task_definition.param_defaults:
            # we can't "add" defaults to the current config and rely on configuration system
            # we don't know what section name to use, and it my clash with current config
            return build_parameter_value(
                param_def,
                ConfigValue(
                    self.task_definition.param_defaults.get(param_name),
                    source=self._source_name("default"),
                ),
            )

        if not param_def.is_output():
            # outputs can be none, we "generate" their values later
            err_msg = "No value defined for '{name}' at {context_str}".format(
                name=param_name, context_str=self._ctor_as_str
            )
            raise MissingParameterError(
                err_msg,
                help_msg=param_def._get_help_message(sections=self.config_sections),
            )

        # returning empty Output value
        return ParameterValue(
            parameter=param_def,
            source="",
            source_value=NOTHING,
            value=NOTHING,
            parsed=False,
        )

    # should refactored out
    def _get_task_config_sections(self, config, task_config_sections_extra):
        # [HIGHEST PRIORITY, ... , LOWEST PRIORITY]
        sections = [self.task_name]

        # '_from=...' at config files
        sections.extend(get_task_from_sections(config, self.task_name))

        # sections by family
        sections.extend(
            [self.task_passport.task_family, self.task_passport.full_task_family]
        )

        # from user
        if task_config_sections_extra:
            sections.extend(task_config_sections_extra)

        from dbnd._core.task.config import Config

        # adding "default sections"  - LOWEST PRIORITY
        if issubclass(self.task_cls, _TaskParamContainer):
            sections += [CONF_TASK_SECTION]
        elif issubclass(self.task_cls, Config):
            sections += [CONF_CONFIG_SECTION]

        # dedup values and preserve order
        sections = map(lambda x: x.lower(), filter(None, sections))
        sections = list(unique_everseen(sections))

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

        task_enabled = True
        if self.parent_task and not self.parent_task.ctrl.should_run():
            task_enabled = False

        # load from task_band if exists
        task_band_param = task_params.get_param_value(TASK_BAND_PARAMETER_NAME)
        if task_band_param and task_band_param.value:
            task_band = task_band_param.value

            # we are going to load all task parameters from task_band
            task_params = self.load_task_params_from_task_band(task_band, task_params)

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

        task_children_scope_params = self._calculate_task_children_scope_params(
            task_params=task_params
        )

        task = task_metaclass._build_task_obj(
            task_definition=self.task_definition,
            task_name=self.task_name,
            task_params=task_params,
            task_signature_obj=task_signature_obj,
            task_config_override=self.task_config_override,
            task_config_layer=self.config.config_layer,
            task_enabled=task_enabled,
            task_sections=self.config_sections,
            task_children_scope_params=task_children_scope_params,
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
        calculates parameter.target_config based on extra config
        at `parameter_name__target value
        user might want to change the target_config for specific param using configuration/cli
        """

        # used for target_format update
        # change param_def definition based on config state
        target_def_key = "%s__target" % param_def.name
        target_config = self.config.get_multisection_config_value(
            self.config_sections, key=target_def_key
        )
        if not target_config:
            return param_def

        # the last is from a higher level
        target_config = last(target_config)
        try:
            target_config = parse_target_config(target_config.value)
        except Exception as ex:
            raise param_def.parameter_exception(
                "Calculate target config for %s : target_config='%s'"
                % (target_def_key, target_config.value),
                ex,
            )

        param_def = param_def.modify(target_config=target_config)
        return param_def

    def _build_task_param_values(self):
        """
        This process is composed from those parts:
        1. Adding extra layers of config (based on defaults, task_config and override)
        2. Building the params
        3. Validating the params
        """
        # copy because we about to edit it
        params_to_build = self.task_definition.task_param_defs.copy()
        task_param_values = []

        # let's start to apply layers on current config
        # at the end of the build we will save current layer and will make it "active" config
        # during task execution/initializaiton

        # SETUP: Task may have TaskClass.defaults= {...} - LOWEST priority
        # add these values, so task build or nested tasks/configs builds will see it
        task_defaults = self.task_definition.task_defaults_config_store
        if task_defaults:
            # task_defaults are processed at TaskDefinition,
            # it's ConfigStore with priority = ConfigValuePriority.FALLBACK,
            self.config.set_values(
                config_values=task_defaults, source=self._source_name("task.defaults")
            )

        # SETUP: build and apply any Task class level config
        # User has specified "task_config = ..."
        # Only orchestration tasks has this property
        param_task_config = params_to_build.pop("task_config", None)
        if param_task_config:
            task_param_values.append(
                self._calculate_and_add_layer_of_task_config(param_task_config)
            )

        # SETUP: Apply all "override" values in Task(override={})
        # This is highest priority config, should deprecated in favor of "with config(..)"
        if self.task_config_override:
            self.config.set_values(
                source=self._source_name("ctor[override]"),
                config_values=self.task_config_override,
                priority=ConfigValuePriority.OVERRIDE,
            )

        # SETUP: build and set EnvConfig
        param_task_env = params_to_build.pop("task_env", None)
        if param_task_env:
            # we apply them to config only if there are no values (this is defaults)
            """
            find same parameters in the current task class and EnvConfig
            and take the value of that params from environment
            for example  spark_config in SparkTask  (defined by EnvConfig.spark_config)
            we take values using names only
            """
            value_task_env_config = self._build_parameter_value(param_task_env)
            self.task_env_config = value_task_env_config.value  # type: EnvConfig
            task_param_values.append(value_task_env_config)

        # log with the final config
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

    def _calculate_task_children_scope_params(self, task_params):
        # type: (Parameters)-> Dict[str, ParameterValue]
        """
        Task can get the value for some params from Parent task (parent of any level)
        If param is defined at parent with scope=ParameterScope.children, it will be added to
        task_children_scope_params and will be used at param calculation

        for example:
          class ChildTask(Task):
              my_p = parameter() # it doesn't have to be scoped
          class ParentTask(Task):
              my_p = parameter(scope=ParameterScope.children)
            def band:
              my_p_task = ChildTask() # no explicit "wiring" of my_p


        """
        if not self.task_cls._conf__scoped_params:
            return {}

        if self.parent_task and self.parent_task.task_children_scope_params:
            # if we have parent - we need to use as a baseline
            task_children_scope_params = (
                self.parent_task.task_children_scope_params.copy()
            )
        else:
            task_children_scope_params = {}

        for p_val in task_params.get_param_values():
            #  we want only parameters of the right scope -- children
            if p_val.parameter.scope == ParameterScope.children:
                task_children_scope_params[p_val.name] = p_val
        return task_children_scope_params

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
        if self.task_cls.task_decorator is not None:
            callable_spec = self.task_cls.task_decorator.get_callable_spec()
            # only in functions we can have args as we know exact "call" signature
            task_args, task_kwargs = args_to_kwargs(
                callable_spec.args, task_args, task_kwargs
            )
            has_varargs = callable_spec.varargs
            has_varkwargs = callable_spec.varkw

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

    def _calculate_and_add_layer_of_task_config(self, param_task_config):
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

    def _source_name(self, name):
        return self.task_definition.task_passport.format_source_name(name)

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
