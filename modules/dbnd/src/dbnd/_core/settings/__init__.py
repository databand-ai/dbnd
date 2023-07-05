# © Copyright Databand.ai, an IBM Company 2022

import typing

from dbnd._core.errors.base import ConfigLookupError
from dbnd._core.settings.core import CoreConfig, DatabandSystemConfig
from dbnd._core.settings.histogram import HistogramConfig  # noqa: F401
from dbnd._core.settings.run_info import RunInfoConfig  # noqa: F401
from dbnd._core.settings.tracking_config import TrackingConfig
from dbnd._core.settings.tracking_log_config import TrackingLoggingConfig
from dbnd._core.task import Config


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext


class DatabandSettings(object):
    def __init__(self, databand_context):
        super(DatabandSettings, self).__init__()
        self.databand_context: "DatabandContext" = databand_context

        self.core = CoreConfig()
        self.tracking = TrackingConfig()
        self.tracking_log = TrackingLoggingConfig()

        self.singleton_configs = {}

    @property
    def system(self):
        # type:()->DatabandSystemConfig
        return self.databand_context.system_settings

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
