from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.config import Config


# we are going to use this config to build objects
# however, we should build this config using the same mechanism
# in order to prevent "recursion", we provide fake object,
# that is completly agnostic to Config


class _TaskBuilderConfigInitial(object):
    verbose = False
    sign_with_full_qualified_name = False
    sign_with_task_code = False


class TaskBuilderConfig(Config):
    """(Advanced) Databand's core task builder"""

    _conf__task_family = "task_build"

    verbose = parameter.value(False)

    sign_with_full_qualified_name = parameter.value(False)

    sign_with_task_code = parameter.value(False)
