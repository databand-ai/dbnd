# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.errors.friendly_error.helpers import (
    _parameter_name,
    _run_name,
    _task_name,
)


def failed_to_save_value_to_target(ex, task, parameter, target, value):
    if parameter.name == "result":
        return DatabandRuntimeError(
            "Can't save return value of %s to %s': %s"
            % (_parameter_name(task, parameter), target, ex),
            show_exc_info=True,
            nested_exceptions=[ex],
            help_msg="Check your {} return value "
            "and it definition ('{}')".format(
                _run_name(task), parameter.value_type.type
            ),
        )
    return DatabandRuntimeError(
        "Can't save %s to %s': %s" % (_parameter_name(task, parameter), target, ex),
        show_exc_info=True,
        nested_exceptions=[ex],
        help_msg="Check your %s logic. " % task.friendly_task_name,
    )


def failed_to_read_value_from_target(ex, task, parameter, target):
    return DatabandRuntimeError(
        "Can't read %s from %s': %s" % (_parameter_name(task, parameter), target, ex),
        show_exc_info=True,
        nested_exceptions=[ex],
        help_msg="Check your %s logic. " % task.friendly_task_name,
    )


def failed_to_assign_result(task, result_parameter):
    return DatabandRuntimeError(
        "The result of the band/run call is None, "
        "it can not be assigned to {schema}".format(
            task=task, schema=result_parameter.schema
        ),
        help_msg="Check your %s return value" % (_task_name(task)),
    )


def failed_to_process_non_empty_result(task, result):
    return DatabandRuntimeError(
        "Can' process non empty result of {task} while it's marked as task without outputs: result={result}".format(
            task=_task_name(task), result=result
        ),
        help_msg="Please, use @task(result=YOU RESULT SCHEMA)",
    )


def can_run_only_tasks(task):
    return DatabandRuntimeError(
        "Databand can run only Tasks, got {task} instead".format(task=type(task)),
        help_msg="Please, use check that you don't call function while providing it to databand. "
        "Use YOUR_TASK_FUNCTION.task()",
    )


def wrong_return_value_type(task_def, names, result):
    return DatabandRuntimeError(
        "Returned value from '{task}' should be tuple/list/dict as task has multiple result."
        "Expected tuple of '{names}', got value of type '{result}'".format(
            task=task_def.run_name(), names=names, result=type(result)
        )
    )


def wrong_return_value_len(task_def, names, result):
    return DatabandRuntimeError(
        "Returned result from '{task}' doesn't match expected schema. "
        "Expected tuple of '{names}', got tuple of length '{result}'".format(
            task=task_def.run_name(), names=names, result=len(result)
        )
    )


def failed_to_run_spark_script(task, cmd, application, return_code, error_snippets):
    return DatabandRuntimeError(
        "spark_submit failed with return code %s. Failed to run: %s"
        % (return_code, cmd),
        show_exc_info=False,
        nested_exceptions=error_snippets,
        help_msg="Check your  %s logic and input data %s. Inspect spark logs for more info."
        % (application, list(task.relations.task_inputs_user.values())),
    )


def failed_to_run_cmd(name, cmd_str, return_code):
    return DatabandRuntimeError(
        "{name} has failed, returncode='{return_code}'. Failed to run: {cmd}".format(
            name=name, return_code=return_code, cmd=cmd_str
        ),
        show_exc_info=False,
        help_msg="Inspect logs for more info.",
    )


def failed_spark_status(msg):
    return DatabandRuntimeError(msg, show_exc_info=False)


def failed_to_run_emr_step(reason, logs_path, error_snippets):
    if logs_path and error_snippets:
        return DatabandRuntimeError(
            "EMR Spark step failed with reason: %s " % reason,
            show_exc_info=False,
            nested_exceptions=error_snippets,
            help_msg="Check your application logic. Inspect spark emr logs for more info.\n "
            "Logs are available at %s." % logs_path,
        )
    return DatabandRuntimeError(
        "EMR Spark step failed with reason: %s. Additionally Databand failed to get EMR logs."
        % reason,
        show_exc_info=False,
        help_msg="Check your application logic. Inspect emr console for logs and more info\n ",
    )


def system_exit_at_task_run(task, ex):
    return DatabandRuntimeError(
        "Task execution has been aborted with sys.exit() call: %s" % ex,
        nested_exceptions=ex,
        show_exc_info=False,
        help_msg="Check your task run()\n ",
    )


def databand_context_killed(interrupt_location):
    return DatabandRuntimeError(
        "Databand Context has been killed externaly. Interrupted at %s",
        interrupt_location,
    )
