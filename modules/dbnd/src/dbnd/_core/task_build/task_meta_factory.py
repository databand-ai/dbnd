import copy
import logging
import typing

from itertools import chain

from more_itertools import unique_everseen
from six import iteritems

from dbnd._core.configuration.config_path import (
    CONF_CONFIG_SECTION,
    CONF_TASK_ENV_SECTION,
    CONF_TASK_SECTION,
)
from dbnd._core.configuration.config_readers import (
    is_like_config_store,
    parse_as_config_store,
)
from dbnd._core.configuration.config_value import ConfigValue
from dbnd._core.constants import RESULT_PARAM, ParamValidation, _TaskParamContainer
from dbnd._core.decorator.task_decorator_spec import args_to_kwargs
from dbnd._core.errors import MissingParameterError, friendly_error
from dbnd._core.parameter import build_user_parameter_value
from dbnd._core.parameter.parameter_definition import (
    ParameterScope,
    build_parameter_value,
)
from dbnd._core.parameter.parameter_value import ParameterValue
from dbnd._core.task_build.multi_section_config import MultiSectionConfig
from dbnd._core.task_build.task_context import try_get_current_task
from dbnd._core.task_build.task_passport import format_source_suffix
from dbnd._core.task_build.task_signature import TASK_ID_INVALID_CHAR_REGEX
from dbnd._core.task_ctrl.task_meta import TaskMeta
from dbnd._core.utils.basics.text_banner import safe_string
from targets import target
from targets.target_config import parse_target_config


if typing.TYPE_CHECKING:
    from typing import List, Type, Any, Dict
    from dbnd import ParameterDefinition
    from dbnd._core.configuration.dbnd_config import DbndConfig
    from dbnd._core.settings import EnvConfig
    from dbnd._core.task.base_task import _BaseTask
    from dbnd._core.task_build.task_definition import TaskDefinition

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


class BaseTaskMetaFactory(object):
    def __init__(self, config, task_cls, task_args, task_kwargs):
        # type:(DbndConfig, Type[_BaseTask], Any, Any)->None
        self.task_cls = task_cls
        self.task_definition = task_cls.task_definition  # type: TaskDefinition

        # keep copy of user inputs
        self.task_kwargs__ctor = task_kwargs.copy()
        self.task_args__ctor = list(task_args)

        self.parent_task = try_get_current_task()

        self.task_family = self.task_definition.task_family
        self.task_name = self.task_family

        self.multi_sec_conf = MultiSectionConfig(config, [])

        self._task_params = self.task_definition.task_params.copy()

        self.ctor_kwargs = {}

        self._exc_desc = self.task_family
        self.task_errors = []

    def build_task_env(self, param_task_env):
        """
        find same parameters in the current task class and EnvConfig
        and take the value of that params from environment
        for example  spark_config in SparkTask  (defined by EnvConfig.spark_config)
        we take values using names only
        """
        value_task_env = self.build_parameter_value(param_task_env)
        env_config = value_task_env.value  # type: EnvConfig

        param_values = []
        #  we want only parameters of the right scope -- children
        for param_def, param_value in env_config._params.get_param_values(
            scope=ParameterScope.children
        ):
            param_values.append((param_def.name, param_value))

        self.multi_sec_conf.update_section(
            CONF_TASK_ENV_SECTION,
            param_values=param_values,
            source=self._source_name("env[%s]" % env_config.task_name),
        )

        return value_task_env

    def build_task_config(self, param_task_config):
        """
        calculate any task class level params and updates the config with thier values
        @pipeline(task_config={CoreConfig.tracker: ["console"])
        <or>
        class a(Task):
            task_config = {"core": {"tracker": ["console"]}}
            <or>
            task_config = {CoreConfig.tracker: ["console"]}
        """
        param_task_config_value = self.build_parameter_value(param_task_config)
        if param_task_config_value.value:
            # we want the ability to have dict of parameter definitions
            # mapping to values - to set as config if needed.
            # but parameters value which are dicts can't have non string as a key
            value = param_task_config_value.value
            if value and is_like_config_store(value):
                param_task_config_value.value = parse_as_config_store(value)

            # merging `Task.task_config` into current configuration
            self.multi_sec_conf.set_values(
                param_task_config_value.value, source=self._source_name("task_config"),
            )

        return param_task_config_value

    def build_parameter_values(self, params):
        # type: (List[ParameterDefinition]) -> List[ParameterValue]
        result = []
        for param_def in params:
            try:
                p_value = self.build_parameter_value(param_def)
                result.append(p_value)
            except MissingParameterError as ex:
                self.task_errors.append(ex)
        return result

    def build_parameter_value(self, param_def):
        """
        This is the place we calculate param_def value
        based on Class(defaults, constructor, overrides, root)
        and Config(env, cmd line, config)  state
        """

        # used for target_format update
        # change param_def definition based on config state
        param_def = self._update_param_def_target_config(param_def=param_def)
        param_name = param_def.name
        p_config_value = self.multi_sec_conf.get_param_config_value(param_def)

        # first check for override
        if p_config_value and p_config_value.override:
            cf_value = p_config_value

        # second using kwargs we received from the user
        elif param_name in self.ctor_kwargs:
            cf_value = ConfigValue(
                self.ctor_kwargs.get(param_name), source=self._source_name("ctor")
            )

        elif p_config_value:
            cf_value = p_config_value

        elif param_def.is_output():
            # outputs can be none, we "generate" their values later
            cf_value = None

        else:
            err_msg = "No value defined for '{name}' at {context_str}".format(
                name=param_name, context_str=self._exc_desc
            )
            raise MissingParameterError(
                err_msg,
                help_msg=param_def._get_help_message(
                    sections=self.multi_sec_conf.sections
                ),
            )

        return build_parameter_value(param_def, cf_value)

    def _update_param_def_target_config(self, param_def):
        """calculates parameter.target_config based on extra config at parameter__target value"""
        target_option = "%s__target" % param_def.name
        target_config = self.multi_sec_conf.get_config_value(key=target_option)
        if not target_config:
            return param_def

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

    def _source_name(self, name):
        return self.task_definition.task_passport.format_source_name(name)

    # should refactored out
    def _get_task_multi_section_config(self, config, task_kwargs):
        # there is priority of task name over task family, as name is more specific
        sections = [self.task_name]

        # _from at config files
        sections.extend(get_task_from_sections(config, self.task_name))

        # sections by family
        sections.extend([self.task_family, self.task_definition.full_task_family])

        kwargs_task_config_sections = task_kwargs.pop("task_config_sections", None)
        if kwargs_task_config_sections:
            sections.extend(kwargs_task_config_sections)

        # adding "default sections"  - LOWEST PRIORITY
        if issubclass(self.task_definition.task_class, _TaskParamContainer):
            sections += [CONF_TASK_SECTION]

        from dbnd._core.task.config import Config

        if issubclass(self.task_definition.task_class, Config):
            sections += [CONF_CONFIG_SECTION]

        # dedup the values
        sections = list(unique_everseen(filter(None, sections)))

        return MultiSectionConfig(config, sections)


class TaskMetaFactory(BaseTaskMetaFactory):
    """
     we have current config at  dbnd_config
     every tasks checks:
     1. overrides
     2. regular
     for (1) and (2) we are going to check following sections:
     A. it's own section    (task_family)
     if task have some child properties - it can affect it by "task" section


     1. overrides,
     2. constructor
     3. task_config
     4. configuration

     How to override specific task sub configs
     task_config = {  "spark" : { "param" : "ss"  }
     task_config = { spark.jars = some_jars ,
                     kubernetes.gpu = some_gpu }

    """

    def __init__(self, config, task_cls, task_args, task_kwargs):
        # type:(DbndConfig, Type[_BaseTask], Any, Any)->None
        super(TaskMetaFactory, self).__init__(
            config=config,
            task_cls=task_cls,
            task_args=task_args,
            task_kwargs=task_kwargs,
        )

        self.task_factory_config = TaskFactoryConfig.from_dbnd_config(config)
        self.verbose_build = self.task_factory_config.verbose

        self.task_family = task_kwargs.pop("task_family", self.task_family)
        # extra params from constructor
        self.task_name = task_kwargs.pop("task_name", None)

        self.task_config_override = task_kwargs.pop("override", None) or {}
        self.task_kwargs = task_kwargs

        if self.task_name:
            self.task_name = TASK_ID_INVALID_CHAR_REGEX.sub("_", self.task_name)

        # user gives explicit name, or it full_task_family
        self.task_main_config_section = (
            self.task_name or self.task_definition.task_config_section
        )

        if self.task_name is None:
            self.task_name = self.task_family

        self.multi_sec_conf = self._get_task_multi_section_config(config, task_kwargs)

        self.ctor_kwargs = None
        # utilities section
        self.build_warnings = []
        self._exc_desc = "%s(%s)@%s" % (
            self.task_family,
            ", ".join(
                (
                    "%s=%s" % (p, safe_string(repr(k), 300))
                    for p, k in iteritems(self.task_kwargs__ctor)
                )
            ),
            str(self.task_cls),
        )
        self.task_errors = []

    def create_dbnd_task_meta(self):
        # create task meta
        self._log_build_step(
            "Resolving task params with %s" % self.multi_sec_conf.sections
        )
        try:
            task_meta = self.build_task_meta()
        except Exception:
            self._log_config(force_log=True)
            raise
        self._log_build_step("Task Meta with obj_id = %s" % str(task_meta.obj_key))
        return task_meta

    def build_task_meta(self):
        """
        This process is composed from those parts:
        1. Editing the config while building params - depend on some magic params
        2. Validating the params
        3. Building from task_band if needed
        4. Editing the configuration again for child scope params
        """
        # First magic: let apply all "override" values in Task(override={})
        if self.task_config_override:
            self.multi_sec_conf.set_values(
                source=self._source_name("override"),
                config_values=self.task_config_override,
                override=True,
            )

        # log with the updated config
        self._log_config()

        # validate and update the kwargs used for building the param values
        self.ctor_kwargs = self._build_task_ctor_kwargs(
            self.task_args__ctor, self.task_kwargs
        )

        # copy because we about to edit it
        params_to_build = self._task_params.copy()
        task_param_values = []

        # Second magic: build and apply any Task class level config
        param_task_config = params_to_build.pop("task_config", None)
        if param_task_config:
            task_config = self.build_task_config(param_task_config)
            task_param_values.append(task_config)

        # Third magic: build and apply Environment level config
        param_task_env = params_to_build.pop("task_env", None)
        if param_task_env:
            task_env = self.build_task_env(param_task_env)
            task_param_values.append(task_env)

        # calculate configuration per parameter, and calculate parameter value
        regular_params = self.build_parameter_values(params_to_build.values())

        task_param_values.extend(regular_params)
        task_param_values = {param.name: param for param in task_param_values}

        # validation for errors and log warnings
        self.validate_no_extra_config_params(task_param_values)

        if self.task_errors:
            self._raise_task_build_errors()

        if self.build_warnings:
            self._log_task_build_warnings()

        task_enabled = True
        if self.parent_task:
            task_enabled = self.parent_task.ctrl.should_run()

        # load from task_band if exists TODO: move somewhere else
        task_band_param = task_param_values.get(TASK_BAND_PARAMETER_NAME, None)
        if task_band_param and task_band_param.value:
            task_band = task_band_param.value

            # we are going to load task from band
            task_enabled = False
            task_param_values = self.load_task_params_from_task_band(
                task_band, task_param_values
            )

        # update [task] section with Scope.children params
        if not self.task_cls._conf__no_child_params:
            self._apply_task_children_scope(task_param_values=task_param_values)

        return TaskMeta(
            task_definition=self.task_definition,
            task_family=self.task_family,
            task_name=self.task_name,
            task_params=task_param_values,
            task_config_override=self.task_config_override,
            config_layer=self.multi_sec_conf.layer,
            task_enabled=task_enabled,
            task_sections=self.multi_sec_conf.sections,
        )

    def validate_no_extra_config_params(self, task_param_values):
        if "validate_no_extra_params" not in task_param_values:
            return

        validate_no_extra_params = task_param_values["validate_no_extra_params"].value
        if validate_no_extra_params == ParamValidation.disabled:
            return

        # Must lower task parameter name to comply to case insensitivity of configuration
        task_param_names = {name.lower() for name in self._task_params}

        for key, value in self.multi_sec_conf.items():
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

    def _apply_task_children_scope(self, task_param_values):
        # type: (Dict[str,ParameterValue])-> None

        param_values = []
        for p_val in task_param_values.values():
            #  we want only parameters of the right scope -- children
            if p_val.parameter.scope == ParameterScope.children:
                param_values.append((p_val.name, p_val.value))

        self.multi_sec_conf.update_section(
            CONF_TASK_SECTION,
            param_values=param_values,
            source=self._source_name(ParameterScope.children.value),
        )

    def _build_task_ctor_kwargs(self, task_args, task_kwargs):
        param_names = set(self._task_params)

        args_orig, kwargs_orig = list(task_args), task_kwargs.copy()
        has_varargs = False
        has_varkwargs = False
        if self.task_cls._conf__decorator_spec is not None:
            # only in functions we can have args as we know exact "call" signature
            task_args, task_kwargs = args_to_kwargs(
                self.task_cls._conf__decorator_spec.args, task_args, task_kwargs
            )
            has_varargs = self.task_cls._conf__decorator_spec.varargs
            has_varkwargs = self.task_cls._conf__decorator_spec.varkw

        # now we should not have any args, we don't know how to assign them
        if task_args and not has_varargs:
            raise friendly_error.unknown_args_in_task_call(
                self.parent_task, self.task_cls, func_params=(args_orig, kwargs_orig)
            )

        if not has_varkwargs:
            # there are no kwargs for the callable
            # we need to be sure that all the kwargs are for parameters
            for key in task_kwargs:
                if key not in param_names:
                    raise friendly_error.task_build.unknown_parameter_in_constructor(
                        constructor=self._exc_desc,
                        param_name=key,
                        task_parent=self.parent_task,
                    )

        return task_kwargs

    def __str__(self):
        if self.task_name == self.task_family:
            return "TaskFactory(%s)" % self.task_name
        return "TaskFactory(%s@%s)" % (self.task_name, self.task_family)

    def load_task_params_from_task_band(self, task_band, task_params):
        task_band_value = target(task_band).as_object.read_json()

        new_params = {}
        found = []
        source = "task_band.json"
        for name, p_value in iteritems(task_params):
            if name not in task_band_value or name == RESULT_PARAM:
                new_params[name] = p_value
                continue

            value = p_value.parameter.calc_init_value(task_band_value[name])
            found.append(name)
            new_parameter_value = ParameterValue(
                parameter=p_value.parameter,
                source=source,
                source_value=value,
                value=value,
            )
            new_params[new_parameter_value.name] = new_parameter_value

        logger.info(
            "Loading task '{task_family}' from {task_band}:\n"
            "\tfields taken:\t{found}".format(
                task_family=self.task_family, task_band=task_band, found=",".join(found)
            )
        )
        return new_params

    def _log_build_step(self, msg, force_log=False):
        if self.verbose_build or force_log:
            logger.info("[%s] %s", self.task_name, msg)

    def _log_config(self, force_log=False):
        msg = self.multi_sec_conf.config_log
        self._log_build_step(msg, force_log=force_log)

    def _raise_task_build_errors(self):
        if len(self.task_errors) == 1:
            raise self.task_errors[0]

        raise friendly_error.failed_to_create_task(
            self._exc_desc, nested_exceptions=self.task_errors
        )

    def _get_task_or_config_string(self):
        from dbnd._core.task.config import Config

        if issubclass(self.task_definition.task_class, Config):
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


class TrackedTaskMetaFactory(BaseTaskMetaFactory):
    def __init__(self, config, task_cls, task_args, task_kwargs):
        # type:(DbndConfig, Type[_BaseTask], Any, Any)->None
        super(TrackedTaskMetaFactory, self).__init__(
            config=config,
            task_cls=task_cls,
            task_args=task_args,
            task_kwargs=task_kwargs,
        )

        self.multi_sec_conf = self._get_task_multi_section_config(config, task_kwargs)

    def create_dbnd_task_meta(self):
        task_args, task_kwargs = self.task_args__ctor, self.task_kwargs__ctor
        # transfer args to kwargs and update
        if self.task_cls._conf__decorator_spec is not None:
            task_args, task_kwargs = args_to_kwargs(
                self.task_cls._conf__decorator_spec.args, task_args, task_kwargs
            )

        # build parameters which defined in the class
        params_to_build = self._task_params.copy()
        class_param_values = []
        # we need to calculate task_env first
        param_task_env = params_to_build.pop("task_env", None)
        if param_task_env:
            task_env = self.build_task_env(param_task_env)
            class_param_values.append(task_env)

        class_param_values.extend(self.build_parameter_values(params_to_build.values()))

        # build parameters which received from the user
        user_param_values = self._build_user_parameter_values(task_args, task_kwargs)

        # merge class and user params
        task_params = {p.name: p for p in class_param_values}
        task_params.update(user_param_values)

        return TaskMeta(
            task_definition=self.task_definition,
            task_family=self.task_family,
            task_name=self.task_name,
            task_params=task_params,
            task_config_override={},
            config_layer=self.multi_sec_conf.layer,
            task_sections=self.multi_sec_conf.sections,
        )

    def _build_user_parameter_values(self, task_args, task_kwargs):
        """
        In tracking task we need to build params without definitions.
        Those params value need no calculations and therefore are very easy to construct
        """
        args = ((str(i), value) for i, value in enumerate(task_args))
        kwargs = iteritems(task_kwargs)

        values = {}
        for name, value in chain(args, kwargs):
            values[name] = build_user_parameter_value(
                name, value, source=self.task_definition.full_task_family_short
            )

        return values
