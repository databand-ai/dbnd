# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.constants import TaskEssence
from dbnd._core.errors.base import ConfigLookupError
from dbnd._core.task.task_with_params import _TaskWithParams
from dbnd._core.utils.basics.singleton_context import SingletonContext


class Config(_TaskWithParams, SingletonContext):
    """
    Class for configuration, See ``ConfigClasses``.

    The nice thing is that we can instantiate this class
    and get an object with all the environment variables set.
    This is arguably a bit of a hack.
    """

    task_essence = TaskEssence.CONFIG

    # we need this empty constructor, to support "any" parameters at the ctor auto-completion
    def __init__(self, **kwargs):
        super(Config, self).__init__(**kwargs)

    def __str__(self):
        return self.task_name

    @classmethod
    def from_databand_context(cls, name=None):
        """Syntax sugar for accessing the current config instance."""
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


Config.task_definition.hidden = True
