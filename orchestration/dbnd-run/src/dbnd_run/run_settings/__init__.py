# © Copyright Databand.ai, an IBM Company 2022

import typing

from typing import Union

from dbnd._core.errors import DatabandConfigError
from dbnd._core.errors.base import ConfigLookupError
from dbnd._core.settings.core import DatabandSystemConfig
from dbnd._core.settings.histogram import HistogramConfig  # noqa: F401
from dbnd._core.settings.run_info import RunInfoConfig  # noqa: F401
from dbnd._core.task import Config
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd_run.run_settings.describe import DescribeConfig
from dbnd_run.run_settings.engine import EngineConfig  # noqa: F401
from dbnd_run.run_settings.env import EnvConfig, LocalEnvConfig  # noqa: F401
from dbnd_run.run_settings.git import GitConfig
from dbnd_run.run_settings.log import RunLoggingConfig
from dbnd_run.run_settings.output import OutputConfig
from dbnd_run.run_settings.run import RunConfig
from dbnd_run.run_settings.scheduler import SchedulerConfig


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext


class RunSettings(object):
    def __init__(self, databand_context):
        super(RunSettings, self).__init__()
        self.databand_context = databand_context  # type: DatabandContext

        from dbnd._core.tracking.registry import register_store
        from dbnd_run.orchestration_tracking.backends.tracking_store_file import (
            FileTrackingStore,
        )

        register_store("file", FileTrackingStore)

        self.run = RunConfig()

        self.env = self.get_env_config(self.system.env)
        databand_context.config.set_values(
            config_values={"task": {"task_env": self.system.env}},
            source="RunSettings.env",
        )

        self.git = GitConfig()

        self.describe = DescribeConfig()

        self.output = OutputConfig()
        self.run_logging = RunLoggingConfig()
        self.scheduler = SchedulerConfig()

        self.singleton_configs = {}

        self.user_configs = {}
        for user_config in self.run.user_configs:
            self.user_configs[user_config] = build_task_from_config(user_config)

    @property
    def system(self):
        # type:()->DatabandSystemConfig
        return self.databand_context.system_settings

    def get_env_config(self, name_or_env):
        # type: ( Union[str, EnvConfig]) -> EnvConfig
        if isinstance(name_or_env, EnvConfig):
            return name_or_env

        enabled_environments = self.run.environments
        if name_or_env not in enabled_environments:
            raise DatabandConfigError(
                "Unknown env name '%s', available environments are %s,  please enable it at '[core]environments' "
                % (name_or_env, enabled_environments)
            )
        return build_task_from_config(name_or_env, EnvConfig)

    def get_config(self, name):
        """
        Retrieve a config by name, return existing instance or new one if not created yet.
        The config lookup is using **only the directed subclass of Config object**.
        """
        if hasattr(self, name):
            # dbnd-core configs are attributes of settings.
            # we don't want 2 instances of each config per context.
            config = getattr(self, name)
            if isinstance(config, Config):
                return config

        if name in self.singleton_configs:
            return self.singleton_configs[name]

        # works only for a direct child of Config
        for conf_cls in Config.__subclasses__():
            if conf_cls._conf__task_family == name:
                conf = conf_cls()
                self.singleton_configs[name] = conf
                return conf

        raise ConfigLookupError("Couldn't found config - {}".format(name))
