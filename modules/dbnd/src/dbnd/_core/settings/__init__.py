import typing

from typing import Union

from dbnd._core.errors import DatabandConfigError
from dbnd._core.errors.base import ConfigLookupError
from dbnd._core.plugin.dbnd_plugins import assert_web_enabled
from dbnd._core.settings.core import (
    CoreConfig,
    DatabandSystemConfig,
    DynamicTaskConfig,
    FeaturesConfig,
    TrackingConfig,
)
from dbnd._core.settings.describe import DescribeConfig
from dbnd._core.settings.engine import EngineConfig
from dbnd._core.settings.env import EnvConfig, LocalEnvConfig
from dbnd._core.settings.git import GitConfig
from dbnd._core.settings.histogram import HistogramConfig
from dbnd._core.settings.log import LoggingConfig
from dbnd._core.settings.output import OutputConfig
from dbnd._core.settings.run import RunConfig
from dbnd._core.settings.run_info import RunInfoConfig
from dbnd._core.settings.scheduler import SchedulerConfig
from dbnd._core.task import Config
from dbnd._core.task_build.task_registry import build_task_from_config


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext


class DatabandSettings(object):
    def __init__(self, databand_context):
        super(DatabandSettings, self).__init__()
        self.databand_context = databand_context  # type: DatabandContext
        self.singleton_configs = {}

        self.user_configs = {}
        for user_config in self.core.user_configs:
            self.user_configs[user_config] = build_task_from_config(user_config)

        self._web = None

    @property
    def system(self):
        # type:()->DatabandSystemConfig
        return self.databand_context.system_settings

    @property
    def web(self):
        assert_web_enabled()

        if not self._web:
            from dbnd_web.dbnd_configs import WebserverConfig

            self._web = WebserverConfig()
        return self._web

    def get_env_config(self, name_or_env):
        # type: ( Union[str, EnvConfig]) -> EnvConfig
        if isinstance(name_or_env, EnvConfig):
            return name_or_env

        if name_or_env not in self.core.environments:
            raise DatabandConfigError(
                "Unknown env name '%s', available environments are %s,  please enable it at '[core]environments' "
                % (name_or_env, self.core.environments)
            )
        return build_task_from_config(name_or_env, EnvConfig)

    def get_config(self, name):
        """
        Retrieve a config by name, return existing instance or new one if not created yet.
        The config lookup is using **only the directed subclass of Config object**.
        """
        if name in self.singleton_configs:
            return self.singleton_configs[name]

        # works only for a direct child of Config
        for conf_cls in Config.__subclasses__():
            if conf_cls._conf__task_family == name:
                conf = conf_cls()
                self.singleton_configs[name] = conf
                return conf

        raise ConfigLookupError("Couldn't found config - {}".format(name))

    def __getattr__(self, item):
        # quick reminder - __getattr__ is getting called *after* __getattribute__ and looking
        # only inside self__dict__ - so this operation should be safe
        if item in self.__dict__:
            return self.__dict__[item]

        # avoid handling dunderscore attributes
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError("DatabandSettings has no attribute {}".format(item))

        try:
            return self.get_config(item)
        except ConfigLookupError:
            raise AttributeError("DatabandSettings has no attribute {}".format(item))
