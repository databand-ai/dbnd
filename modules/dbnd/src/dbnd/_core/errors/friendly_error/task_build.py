from __future__ import absolute_import

import difflib

from dbnd._core.errors import DatabandBuildError, UnknownParameterError
from dbnd._core.errors.friendly_error.helpers import _band_call_str


def no_databand_context():
    return DatabandBuildError(
        "You are trying to create task without having active Databand context! "
        "You should have databand context while building/running tasks. "
        "You can create one inplace by adding `init_dbnd()` call"
    )


def unknown_parameter_in_constructor(constructor, param_name, task_parent):
    help_msg = "Remove {param_name} from constructor {constructor}".format(
        param_name=param_name, constructor=constructor
    )

    if task_parent:
        help_msg += " at %s method!" % _band_call_str(task_parent)

    return UnknownParameterError(
        "Unknown parameter '{param_name}' at {constructor}".format(
            constructor=constructor, param_name=param_name
        ),
        help_msg=help_msg,
    )


def unknown_parameter_in_config(
    task_name, param_name, source, task_param_names, config_type
):
    close_matches = difflib.get_close_matches(param_name, task_param_names)
    did_you_mean = None
    if close_matches:
        did_you_mean = "Did you mean: %s" % (", ".join(close_matches))
        help_msg = did_you_mean
    else:
        help_msg = "Remove '{param_name}' from the configuration.".format(
            param_name=param_name
        )
    help_msg = (
        help_msg
        + "\nAlternatively you can disabled this validation or set it to warning: validate_no_extra_params = warn/disabled under [{config_type}] in your config".format(
            config_type=config_type
        )
    )

    e = UnknownParameterError(
        "Unknown parameter '{param_name}' (source: {source}) in config for {task_or_section}".format(
            task_name=task_name,
            param_name=param_name,
            source=source,
            task_or_section="section [%s]" % task_name
            if config_type == "config"
            else "task '%s'" % task_name,
        ),
        help_msg=help_msg,
    )
    e.did_you_mean = did_you_mean
    return e


def pipeline_task_has_unassigned_outputs(task, param):
    return DatabandBuildError(
        "You have unassigned output '{param.name}' in task '{task}'".format(
            param=param, task=task
        ),
        help_msg="Check your {band} logic, Add self.{param.name} = SOME_TASK_OUTPUT".format(
            band=_band_call_str(task), param=param
        ),
    )


def failed_to_call_band(ex, task):
    return DatabandBuildError(
        "Failed to call '%s': %s" % (_band_call_str(task), ex),
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="Check your %s logic" % _band_call_str(task),
    )


def failed_to_call(ex, task_cls):
    return DatabandBuildError(
        "Failed to invoke '%s': %s" % (_band_call_str(task_cls), ex),
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="Check your %s logic" % _band_call_str(task_cls),
    )


def failed_to_assign_param_value_at_band(ex, param, value, task):
    return DatabandBuildError(
        "Failed to assign '{value}' to parameter {param.name} at '{band}': {ex}".format(
            band=_band_call_str(task), value=value, ex=ex, param=param
        ),
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="Check your %s logic" % _band_call_str(task),
    )


def failed_to_build_output_target(p_name, task, ex):
    help_msg = (
        "Make sure that output parameter '{p_name}' is "
        "configured without keyword [parameter]!"
    ).format(p_name=p_name)
    return DatabandBuildError(
        (
            "Failed to build output target for parameter '{p_name}' in task '{task}'! "
            "Exception: {ex}"
        ).format(p_name=p_name, task=task, ex=ex),
        help_msg=help_msg,
    )


def iteration_over_task(task):
    help_msg = "You can iterate over task results, but not the task itself"
    if hasattr(task, "result"):
        help_msg = (
            "If you want to access task results, use {task}().result notation."
            " Probably this task has outputs defined via return value as well as regular parameter".format(
                task=task.get_task_family()
            )
        )
    # we have to return type error,
    # as sometimes we traverse task..and we need indication that it can't be  traversed
    # we can't use BuildError here
    return TypeError(
        "Task '{task}' can not be unpacked or iterated. {help_msg}".format(
            task=task.get_task_family(), help_msg=help_msg
        )
    )


def failed_to_import_pyspark(task, ex):
    return DatabandBuildError(
        "Tried to create spark session for a task {task} but failed to import pyspark".format(
            task=task.get_task_family
        ),
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="Check your environment for pyspark installation.",
    )


def failed_to_access_dbnd_home(dbnd_home, ex):
    return DatabandBuildError(
        (
            "Failed to access DBND_HOME '{dbnd_home}'. "
            "Check that folder exists and a process has sufficient permissions to write there. "
            "Exception: {ex}"
        ).format(dbnd_home=dbnd_home, ex=ex)
    )


def incomplete_output_found_for_task(task_name, complete_outputs, incomplete_outputs):
    return DatabandBuildError(
        "Task {} has incomplete outputs! "
        "This means the task might fail every time. "
        "Complete outputs: {} "
        "Incomplete outputs: {} "
        "Hint: clean the environment or overwrite the output. "
        "To ignore this error, turn off 'validate_task_outputs_on_build' in the '[run]' configuration section".format(
            task_name,
            ", ".join(map(str, complete_outputs)),
            ", ".join(map(str, incomplete_outputs)),
        )
    )
