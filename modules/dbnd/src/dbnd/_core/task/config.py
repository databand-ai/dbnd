from dbnd._core.constants import TaskEssence
from dbnd._core.errors.base import ConfigLookupError
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.utils.basics.singleton_context import SingletonContext


class Config(_BaseTask, SingletonContext):
    """
    Class for configuration. See :ref:`ConfigClasses`.
    The nice thing is that we can instantiate this class
    and get an object with all the environment variables set.
    This is arguably a bit of a hack.
    """

    task_essence = TaskEssence.CONFIG

    def __init__(self, **kwargs):
        super(Config, self).__init__(**kwargs)
        for name, p_value in self.task_meta.task_params.items():
            setattr(self, name, p_value.value)

    def __str__(self):
        return self.task_name

    @classmethod
    def current(cls, name=None):
        """
        Syntax sugar for accessing the current config instance.
        """
        from dbnd._core.current import get_databand_context

        if not name:
            if cls._conf__task_family:
                # using the current cls section name to get the current instance of a class
                name = cls._conf__task_family
            else:
                raise ConfigLookupError(
                    "name is required for retrieving a config instance"
                )

        return get_databand_context().settings.get_config(name)

    def _get_param_value(self, param_name):
        return getattr(self, param_name)


Config.task_definition.hidden = True

config_singletons = {}
