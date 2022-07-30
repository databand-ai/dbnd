# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors import DatabandConfigError, TaskClassNotFoundException


def wrong_type_for_task(section, task_cls, expected_type):
    return DatabandConfigError(
        "Your config '{section}' should be derived from '{expected_type}. Got {task_cls}".format(
            section=section, expected_type=expected_type, task_cls=task_cls
        ),
        help_msg="Make sure '{section}' section in your configuration is of the correct type! (check [{"
        "section}]._type and [{section}]._from ".format(
            section=section,
            task_cls=task_cls,
            # expected_type=expected_type,
        ),
    )


def task_not_exist(task_name, alternative_tasks=None, module=None):
    err_msg = "Could not find the requested task/function '%s'" % task_name
    if module:
        err_msg += " in the current module %s." % module
    if alternative_tasks:
        err_msg += "\nDid you mean to call one of the following:\n %s" % "\n ".join(
            alternative_tasks
        )

    err_msg += (
        "\nPlease check the requested name, make sure the correct module installed, validate the value or add [%s]_type=EXPECTEDTYPE to configuration"
        % task_name
    )

    return TaskClassNotFoundException(
        err_msg,
        help_msg="Validate that this method exist in the requested module. "
        "Make sure you typed the function name correctly.",
        show_exc_info=False,
    )
