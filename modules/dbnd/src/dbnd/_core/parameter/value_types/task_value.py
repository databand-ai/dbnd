from dbnd._core.constants import _TaskParamContainer
from dbnd._core.current import get_databand_context, get_settings
from dbnd._core.task import Config
from dbnd._core.task_build.task_registry import (
    build_task_from_config,
    get_task_registry,
)
from targets.values import ValueType


class TaskValueType(ValueType):
    """
    A value that takes another databand task class.

    When used programatically, the parameter should be specified
    directly with the :py:class:`dbnd.tasks.Task` (sub) class. Like
    ``MyMetaTask(my_task_param=my_tasks.MyTask)``. On the command line,
    you specify the :py:meth:`dbnd.tasks.Task.get_task_family`. Like

    .. code-block:: console

            $ dbnd --module my_tasks MyMetaTask --my_task_param my_namespace.MyTask

    Where ``my_namespace.MyTask`` is defined in the ``my_tasks`` python module.

    When the :py:class:`dbnd.tasks.Task` class is instantiated to an object.
    The value will always be a task class (and not a string).
    """

    type = _TaskParamContainer

    def parse_from_str(self, input):
        """
        Parse a task_famly using the :class:`~dbnd._core.register.Register`
        """

        task_cls = get_task_registry().get_task_cls(input)
        return task_cls()

    def to_str(self, cls):
        """
        Converts the :py:class:`dbnd.tasks.Task` (sub) class to its family name.
        """
        return cls.get_task_family()

    def is_type_of(self, value):
        return isinstance(value, _TaskParamContainer)


class ConfigValueType(ValueType):
    type = Config
    support_from_str = True

    def __init__(self, config_cls):
        self.config_cls = config_cls

    def parse_from_str(self, input):
        """
        Parse a task_famly using the :class:`~dbnd._core.register.Register`
        """

        from dbnd._core.settings.env import EnvConfig

        if isinstance(self.config_cls, EnvConfig):
            return get_settings().get_env_config(input)
        return build_task_from_config(input, expected_type=self.config_cls)

    def to_str(self, x):
        """
        Converts the :py:class:`dbnd.tasks.Task` (sub) class to its family name.
        """
        if x:
            return x.task_name
        return super(ConfigValueType, self).to_str(x)

    def is_type_of(self, value):
        return isinstance(value, Config)

    def to_repr(self, x):
        if x:
            return '"%s"' % x.task_name
        return "None"
