import inspect
import logging
import typing

from collections import OrderedDict
from typing import Any, Dict, Type

import six

from six import iteritems

from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.constants import RESULT_PARAM
from dbnd._core.decorator.func_params_builder import FuncParamsBuilder
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.parameter.parameter_builder import ParameterFactory
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _ParameterKind,
)
from dbnd._core.task_build.task_const import _SAME_AS_PYTHON_MODULE
from dbnd._core.task_build.task_params import TaskDefinitionParams
from dbnd._core.utils.basics.nothing import is_defined
from dbnd._core.utils.structures import combine_mappings
from dbnd._core.utils.uid_utils import get_uuid


if typing.TYPE_CHECKING:
    from dbnd._core.task import Task
    from dbnd._core.task_build.task_metaclass import TaskMetaclass

logger = logging.getLogger(__name__)


def _ordered_params(x):
    return OrderedDict(sorted(x.items(), key=lambda y: y[1].parameter_id))


def _short_name(name):
    # type: (str)->str
    """
    from my_package.sub_package  -> m.s
    """
    return ".".join(n[0] if n else n for n in name.split("."))


class TaskDefinition(object):
    """
    This task represents Task Definition

    """

    def __init__(self, task_class, classdict, namespace_at_class_time):
        super(TaskDefinition, self).__init__()

        self.task_definition_uid = get_uuid()
        self.hidden = False

        self.task_class = task_class  # type: Type[Task]
        self.namespace_at_class_time = namespace_at_class_time
        if self.task_class._conf__decorator_spec:
            cls_name = self.task_class._conf__decorator_spec.name
        else:
            cls_name = self.task_class.__name__

        self.full_task_family = "%s.%s" % (task_class.__module__, cls_name)
        self.full_task_family_short = "%s.%s" % (
            _short_name(task_class.__module__),
            cls_name,
        )

        self.task_family = self._build_user_task_family()
        if not self.task_family:
            self.task_family = cls_name
            self.task_config_section = self.full_task_family
        else:
            self.task_config_section = self.task_family

        # the defaults attribute
        self.defaults = dict()  # type: Dict[ParameterDefinition, Any]

        class_task_params, self.defaults = self._calculate_task_class_values(classdict)

        func_params_builder = self._get_func_params(class_task_params)

        # this is the place we add parameters from function definition
        self._task_params = TaskDefinitionParams(
            class_params=self._update_and_evolve_owner(
                func_params_builder.decorator_kwargs_params, class_task_params
            ),
            user_params=self._update_and_evolve_owner(
                func_params_builder.func_spec_params, {}
            ),
            user_result_params=self._update_and_evolve_owner(
                func_params_builder.result_params, {}
            ),
        )  # type: TaskDefinitionParams

        # if we have output params in function arguments, like   f(some_p=parameter.output)
        # the new function can not return the result of return
        result_param = self._task_params.result_param
        self.single_result_output = (
            result_param is not None
            and self._is_result_single_output(
                self.all_task_params.values(), result_param
            )
        )

        defaults = {
            p.name: p.default
            for p in self.all_task_params.values()
            if is_defined(p.default)
        }
        self.task_defaults_config_store = parse_and_build_config_store(
            source="%s[defaults]" % self.full_task_family_short,
            config_values={self.task_config_section: defaults},
            set_if_not_exists_only=True,
        )

        self.task_defaults_config_store.update(
            parse_and_build_config_store(
                source="%s[defaults_section]" % self.full_task_family_short,
                config_values=self.defaults,
            )
        )
        # now, if we have overloads in code ( calculated in task_definition):
        # class T(BaseT):
        #     some_base_t_property = new_value
        if self.task_class._conf__track_source_code:
            self.task_source_code = _get_task_source_code(self.task_class)
            self.task_module_code = _get_task_module_source_code(self.task_class)
            self.task_source_file = _get_source_file(self.task_class)
        else:
            self.task_source_code = None
            self.task_module_code = ""
            self.task_source_file = None

    @property
    def all_task_params(self):
        return self._task_params.all_params

    @property
    def class_params(self):
        return self._task_params.class_params

    @property
    def user_params(self):
        return self._task_params.user_params

    def _calculate_task_class_values(self, classdict):
        # reflect inherited attributes
        params, base_defaults = self._discover_base_attributes()

        defaults = combine_mappings(base_defaults, classdict.get("defaults", None))

        self._update_and_evolve_owner(classdict, params)

        return params, defaults

    def _update_and_evolve_owner(self, func_params, params):
        # let update params with new class attributes
        self._update_params_from_attributes(func_params, params)
        # add parameters config
        evolved_params = self._evolve_with_owner(params)
        return evolved_params

    def _get_func_params(self, base_params):
        func_params_builder = FuncParamsBuilder(
            base_params=base_params,
            decorator_spec=self.task_class._conf__decorator_spec,
        )
        func_params_builder.build_func_params()
        return func_params_builder

    def _evolve_with_owner(self, params):
        evolved = {
            name: param.evolve_with_owner(task_cls=self.task_class, name=name)
            for name, param in six.iteritems(params)
        }
        return _ordered_params(evolved)

    def _build_user_task_family(self):
        if self.task_class._conf__task_family:
            return self.task_class._conf__task_family

        if is_defined(self.task_class.task_namespace):
            namespace = self.task_class.task_namespace
        elif self.namespace_at_class_time == _SAME_AS_PYTHON_MODULE:
            namespace = self.task_class.__module__
        else:
            namespace = self.namespace_at_class_time

        if namespace:
            return "{}.{}".format(namespace, self.task_class.__name__)
        return None

    def _discover_base_attributes(self):
        # type: ()-> (Dict[str,ParameterDefinition], Dict[str, Any])
        params = dict()
        defaults = dict()
        # we process only "direct" inheritance
        # params will contain definition of param, even it's was overrided by the parent task
        for c in reversed(self.task_class.__bases__):  # type: TaskMetaclass
            if not hasattr(c, "task_definition"):
                logger.debug(
                    "you should inherit from Task objects only: %s -> %s ",
                    self.task_class,
                    c,
                )
                continue
            base_schema = c.task_definition  # type: TaskDefinition
            defaults = combine_mappings(defaults, base_schema.defaults)
            params = combine_mappings(params, c.task_definition.all_task_params)

        return params, defaults

    def _update_params_from_attributes(self, classdict, params):
        class_values = dict()

        for a_name, a_obj in iteritems(classdict):
            context = "%s.%s" % (self.task_family, a_name)
            try:
                if isinstance(a_obj, ParameterFactory):
                    params[a_name] = a_obj.build_parameter(context)
                elif isinstance(a_obj, ParameterDefinition):
                    params[a_name] = a_obj
                else:
                    class_values[a_name] = a_obj
            except Exception:
                logger.error("Failed to process %s" % context)
                raise

        for p_name, p_val in iteritems(class_values):
            if p_name not in params:
                continue
            params[p_name] = params[p_name].modify(default=p_val)

    def _is_result_single_output(self, params, result):
        """
        check that task has only one output and it's output is result
         (there can be sub outputs that are part of result)
        """
        if not result:
            return False
        names = result.names if isinstance(result, FuncResultParameter) else []
        for p in params:
            if p.system or p.kind != _ParameterKind.task_output:
                continue
            if p.name in [RESULT_PARAM, "task_band"]:
                continue
            if p.name in names:
                continue
            return False
        return True

    def __str__(self):
        return "TaskDefinition(%s)" % (self.full_task_family)


def _get_task_source_code(task):
    try:
        import inspect

        if hasattr(task, "_conf__decorator_spec") and task._conf__decorator_spec:
            item = task._conf__decorator_spec.item
            return inspect.getsource(item)
        else:
            return inspect.getsource(task)
    except (TypeError, OSError):
        logger.debug("Failed to task source for %s", task)
    except Exception:
        logger.debug("Error while getting task source")
    return "Error while getting task source"


def _get_task_module_source_code(task):
    try:
        return inspect.getsource(inspect.getmodule(task))
    except TypeError:
        logger.debug("Failed to module source for %s", task)
    except Exception:
        logger.exception("Error while getting task module source")
    return "Error while getting task source"


def _get_source_file(task):
    try:
        return inspect.getfile(task.__class__).replace(".pyc", ".py")
    except Exception:
        logger.warning("Failed find a path of source code for task {}".format(task))
    return None
