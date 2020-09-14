from dbnd._core.constants import _ConfigParamContainer
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.utils.basics.singleton_context import SingletonContext


class Config(_BaseTask, SingletonContext, _ConfigParamContainer):
    """
    Class for configuration. See :ref:`ConfigClasses`.
    The nice thing is that we can instantiate this class
    and get an object with all the environment variables set.
    This is arguably a bit of a hack.
    """

    def __str__(self):
        return self.task_name

    def params_to_env_map(self):
        self._params


Config.task_definition.hidden = True
