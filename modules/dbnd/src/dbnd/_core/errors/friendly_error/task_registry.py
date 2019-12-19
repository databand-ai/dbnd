from dbnd._core.errors import DatabandConfigError


def wrong_type_for_task(section, task_cls, expected_type):
    return DatabandConfigError(
        "You config '{section}' should be derived from '{expected_type}. Got {task_cls}".format(
            section=section, expected_type=expected_type, task_cls=task_cls
        ),
        help_msg="Please check your [{section}] _type = value. ".format(
            section=section
        ),
    )
