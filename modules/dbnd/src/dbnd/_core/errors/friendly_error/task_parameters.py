from dbnd._core.errors import DatabandBuildError
from dbnd._core.errors.errors_utils import safe_value


def unknown_value_type_in_parameter(type_):
    from targets.values import get_types_registry

    return DatabandBuildError(
        "The parameter type '{}' is unknown, ".format(type_),
        help_msg="Use one of: {}, or register the new one via  'register_custom_parameter'".format(
            get_types_registry().list_known_types()
        ),
    )


def no_value_type_defined_in_parameter(context):
    from targets.values import get_types_registry

    return DatabandBuildError(
        "The parameter {context} doesn't have type! Please use parameter[YOUR_TYPE] or parameter[object]".format(
            context=context
        ),
        help_msg="Use one of: {}, or register the new type via  'register_custom_parameter'".format(
            get_types_registry().list_known_types()
        ),
    )


def no_value_type_from_default(default_value, context):
    from targets.values import get_types_registry

    return DatabandBuildError(
        "The parameter '{parameter}' has default value '{default_value}' "
        "that cannot be resolved into known type!"
        "Please use '{parameter} = parameter[YOUR_TYPE]' or '{parameter} = parameter[object]'".format(
            default_value=default_value, parameter=context
        ),
        help_msg="Use one of: {}, or register the new type via  'register_custom_parameter'".format(
            get_types_registry().list_known_types()
        ),
    )


def result_and_params_have_same_keys(context, conflict_keys):
    return DatabandBuildError(
        "{context} have same keys in result schema and function args: {conflict_keys}".format(
            context=context, conflict_keys=conflict_keys
        ),
        help_msg="Please, check %s and rename conflicted keys" % context,
    )


def task_env_param_with_no_env(context, key):
    return DatabandBuildError(
        "{context} can't calculate {key} and env is not defined".format(
            context=context, key=key
        ),
        help_msg="Please, check that %s has task_env parameter" % context,
    )


def task_env_param_not_exists_in_env(context, key, env_config):
    return DatabandBuildError(
        "{context} can't calculate {key}, as it doesn't exists in {env_config}".format(
            context=context, key=key, env_config=env_config
        ),
        help_msg="Please, check {context} definition, only parameters defined at {env_config} "
        "can have 'from_task_env_config=True'".format(
            context=context, env_config=env_config
        ),
    )


def failed_to_build_parameter(context, parameter_state, ex):
    return DatabandBuildError(
        "Failed to build parameter '{context}' defined as '{parameter}': {ex}".format(
            context=context, parameter=parameter_state, ex=ex
        ),
        help_msg="Validate parameter implementation at {}".format(context),
        nested_exceptions=[ex],
    )


def failed_to_convert_to_target_type(param, x, ex):
    return DatabandBuildError(
        "Failed to convert '{value}' to {param_type}".format(
            value=safe_value(x), param=param, param_type=param.value_type.type
        ),
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="Check your %s logic" % param.name,
    )


def value_is_not_parameter_cls(value, context=None):
    return DatabandBuildError(
        "Parameter {context} is '{value}', while it should be defined via 'parameter'".format(
            value=value, context=context
        ),
        help_msg="Please, check your implementation (see relevant lines in exception message)",
    )


def sub_type_with_non_structural_value(context, value_type, sub_type):
    return DatabandBuildError(
        "Sub type {sub_type} is not supported by main value type :{value_type}.".format(
            sub_type=sub_type, value_type=value_type
        ),
        help_msg=" Use main value type like List/Set/Dict: {context} = parameter.sub_type(int)[List]",
    )


def dict_in_result_definition(result_deco):
    return DatabandBuildError(
        "Result definition should be tuple/list, we don't support dict in definition, got: {}".format(
            result_deco
        ),
        help_msg="You can use dict for output of the task "
        "{'features': some_output, 'scores': some_outputs} "
        "but not in result=<DEFINITION>. For example: result=('features', 'scores')",
    )


def wrong_result_definition(result_deco):
    return DatabandBuildError(
        "{} should be Parameter of task_output kind , "
        "or one of list/tuple of parameters or str".format(result_deco)
    )
