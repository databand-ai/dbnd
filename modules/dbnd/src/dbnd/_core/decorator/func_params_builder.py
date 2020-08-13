import logging
import typing

from typing import List

import six

from dbnd._core.constants import RESULT_PARAM
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.decorator.task_decorator_spec import (
    guess_func_arg_value_type,
    guess_func_return_type,
)
from dbnd._core.errors import DatabandBuildError, friendly_error
from dbnd._core.parameter import get_parameter_for_value_type
from dbnd._core.parameter.parameter_builder import (
    PARAMETER_FACTORY,
    ParameterFactory,
    build_parameter,
)
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _ParameterKind,
)
from dbnd._core.utils.basics.nothing import NOTHING, is_defined, is_not_defined
from targets.values import get_value_type_of_type
from targets.values.builtins_values import DefaultObjectValueType


if typing.TYPE_CHECKING:
    from dbnd._core.decorator.task_decorator_spec import _TaskDecoratorSpec
logger = logging.getLogger(__name__)


class FuncParamsBuilder(object):
    def __init__(self, base_params, decorator_spec):
        self.base_params = base_params
        self.decorator_spec = decorator_spec  # type: _TaskDecoratorSpec

        self.decorator_kwargs = (
            decorator_spec.decorator_kwargs if decorator_spec else {}
        )

        self.exclude = {RESULT_PARAM, "self"}

        self.decorator_kwargs_params = {}
        self.func_spec_params = {}
        self.result_params = {}

    def _build_func_spec_params(self, decorator_kwargs_params):
        params = {}
        # let go over all kwargs of the functions
        for k in self.decorator_spec.args:
            if (
                k in self.exclude or k in decorator_kwargs_params
            ):  # excluded or processed already
                continue
            context = "%s.%s" % (self.decorator_spec.name, k)

            default = self.decorator_spec.defaults.get(k, NOTHING)
            if isinstance(default, ParameterFactory):
                # it's inplace defition
                # user_param= parameter[str]
                params[k] = build_parameter(default, context=context)
            elif k in self.base_params:
                # we have param definition from "base class"
                # so we just need to provide a default value for the parameter
                # otherwise - do nothing, as we are good
                if is_defined(default):
                    params[k] = default

            else:
                try:
                    # regular value
                    param_value_type = guess_func_arg_value_type(
                        self.decorator_spec, k, default
                    )
                    if param_value_type is None:
                        # fallback to "object"
                        param_value_type = DefaultObjectValueType()

                    param = get_parameter_for_value_type(param_value_type)
                    param = param.default(default)
                    params[k] = build_parameter(param, context=context)
                except Exception:
                    logger.exception("Failed to analyze function arg %s", context)
                    raise
        return params

    def _build_decorator_kwargs_params(self):
        params = {}
        for k, param in six.iteritems(self.decorator_kwargs):
            if k in self.exclude:  # we'll take care of result param later
                continue

            if param is None:
                self.exclude.add(k)
                continue

            context = "%s.%s" % (self.decorator_spec.name, k)

            if k not in self.decorator_spec.args and k not in self.base_params:
                # we have parameter which is not part of real function signature
                # @task(some_unknown_parameter=parameter)
                logger.info(
                    "{} is not part of parameters, creating hidden parameter".format(
                        context
                    )
                )

            if k in self.decorator_spec.defaults:
                if isinstance(self.decorator_spec.defaults[k], ParameterFactory):
                    raise DatabandBuildError(
                        "{}: {} has conlficted definition in function and in decorator itself".format(
                            context, k
                        )
                    )
                if is_defined(param.parameter.default):
                    logger.warning(
                        "Default value conflict between function and @task decorator"
                    )
                param = param.default(self.decorator_spec.defaults.get(k))

            if k not in self.base_params or isinstance(param, ParameterFactory):
                # we are going to build a new parameter
                param = build_parameter(param, context=context)
            params[k] = param
        return params

    def _build_multiple_outputs_result(self, result_deco_spec):
        # type: (List[ParameterFactory]) -> (List[ParameterDefinition], ParameterDefinition)

        context = "{}.{}".format(self.decorator_spec.name, RESULT_PARAM)
        res = []
        for i, lv in enumerate(result_deco_spec):
            lv = build_parameter(lv, context="%s.%s" % (context, i))

            if not lv.name:
                raise DatabandBuildError(
                    "{}[{}]: {} should have name".format(context, i, type(lv))
                )
            if not lv.is_output():
                raise DatabandBuildError(
                    "{}[{}]: {} should marked as output".format(context, i, type(lv))
                )

            res.append(lv)

        param = FuncResultParameter(
            schema=res,
            value_type=DefaultObjectValueType(),
            name=RESULT_PARAM,
            significant=False,
            kind=_ParameterKind.task_output,
        )
        return param

    def _get_result_parameter_part(self, p, name_hint, value_type_hint):
        if isinstance(p, six.string_types):
            # we got a string  - it's name of parameter
            name_hint = p
            p = None
        elif not isinstance(p, ParameterFactory):
            # we got a type  - it's inline type of parameter
            value_type_hint = get_value_type_of_type(p, inline_value_type=True)
            p = None
        else:
            pass

        if p is None:
            if value_type_hint:
                # we create based on value type
                p = get_parameter_for_value_type(value_type_hint)
            else:
                p = PARAMETER_FACTORY

        if p.parameter.name is None and name_hint:
            p = p.modify(name=name_hint)

        if p.parameter.value_type is None and value_type_hint:
            p = p.modify(value_type=value_type_hint)

        return p.output

    def _get_result_parameter(self):
        context = "{}.{}".format(self.decorator_spec.name, RESULT_PARAM)

        return_spec = guess_func_return_type(self.decorator_spec)

        deco_spec = None
        # first of all , let parse the definition we have
        if RESULT_PARAM in self.decorator_kwargs:
            # @task(result=...)
            deco_spec = self.decorator_kwargs[RESULT_PARAM]
            if isinstance(deco_spec, dict):
                raise friendly_error.task_parameters.dict_in_result_definition(
                    deco_spec
                )

            # @task(result=None)
            if deco_spec is None:
                # user explicitly don't want to have result value
                return {}

            if isinstance(deco_spec, six.string_types):
                # we have result = "output1,output2"
                # support both space and comma
                deco_spec = deco_spec.replace(",", " ").split()
                if len(deco_spec) == 1:
                    deco_spec = deco_spec[0]
            elif isinstance(deco_spec, tuple):
                deco_spec = list(deco_spec)

            # user didn't specify - so we don't have any "hints"
            if is_not_defined(return_spec):
                return_spec = None
            elif return_spec is not None:
                # we will use type hints from  "-> ..." spec only if it's has exact match to our params
                return_spec = self._validate_return_spec(deco_spec, return_spec)
        else:
            # we don't have @task(result=)
            if return_spec is None:
                # .. -> None
                # user explicitly don't want to have result value
                return {}
            # let return default parameter ( pickle in @task)
            if is_not_defined(return_spec):
                return build_parameter(self.decorator_spec.default_result, context)

            # so we have something in return speck, let use it
            if isinstance(return_spec, list):
                # we can get names from ->
                deco_spec = [r[0] for r in return_spec]
            else:
                # or we just use default  name
                deco_spec = RESULT_PARAM

        # so now we have 2 cases
        # 1. we have list of results -->
        if isinstance(deco_spec, list):
            result = []
            for i, deco_p in enumerate(deco_spec):
                value_type_hint = None
                if return_spec:
                    _, value_type_hint = return_spec[i]

                deco_p = self._get_result_parameter_part(
                    p=deco_p, name_hint="result_%s" % i, value_type_hint=value_type_hint
                )
                result.append(deco_p)
            param = self._build_multiple_outputs_result(result)

        # 2. we have only one result-->
        else:
            param = self._get_result_parameter_part(
                p=deco_spec, name_hint=RESULT_PARAM, value_type_hint=return_spec
            )
        return build_parameter(param, context)

    def build_func_params(self):
        if not self.decorator_spec:
            return

        # calc auto parameters from function spec
        # it's easy, as we take function value or deco value.
        self.decorator_kwargs_params = self._build_decorator_kwargs_params()
        self.func_spec_params = self._build_func_spec_params(
            self.decorator_kwargs_params
        )
        self.result_params = self._build_func_result_params()

        self._validate_result_params()

    def _validate_result_params(self):
        conflict_keys = set(self.result_params.keys()).intersection(
            set(self.decorator_kwargs_params.keys()) | set(self.func_spec_params.keys())
        )
        if conflict_keys:
            raise friendly_error.task_parameters.result_and_params_have_same_keys(
                self.decorator_spec.name, conflict_keys
            )

    def _build_func_result_params(self):
        # much more complicated , we need to "smart merge" the result
        # and we need the result type
        result_param = self._get_result_parameter()
        result_params = {}
        if result_param:
            result_params[RESULT_PARAM] = result_param
            if isinstance(result_param, FuncResultParameter):
                result_params.update({p.name: p for p in result_param.schema})
        return result_params

    def _validate_return_spec(self, deco_spec, return_spec):
        # validate lengths

        def err(msg):
            logger.warning(
                "%s() return type definitions are different: \n\t"
                "@task(result='%s')\n\t"
                "# type: -> '%s'\n\t error:%s\n\t"
                " @task definition will be used!",
                self.decorator_spec.name,
                deco_spec,
                return_spec,
                msg,
            )

        deco_is_list = isinstance(deco_spec, list)
        return_spec_is_list = isinstance(return_spec, list)
        if not deco_is_list:
            if not return_spec_is_list:
                # just one definition, good!
                return return_spec

            err("function return definition is a list")
            return None

        # deco is list
        if not return_spec_is_list:
            err("function return definition is a single type")
            return None

        if len(deco_spec) != len(return_spec):
            err("number of outputs is different")
            return None
        return return_spec
