# © Copyright Databand.ai, an IBM Company 2022

import datetime

from dbnd import Config, config, parameter
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.config_value import ConfigValuePriority


class AutoloadedConfig(Config):
    _conf__task_family = "autotestconfig"
    param_int = parameter[int]
    param_datetime = parameter[datetime.datetime]

    def get_autoloaded_config(self):
        # override values for task task_auto_config
        override_values = {
            "task_auto_config": {
                "param_int": self.param_int,
                "param_datetime": self.param_datetime,
            }
        }
        return parse_and_build_config_store(
            config_values=override_values,
            priority=ConfigValuePriority.OVERRIDE,
            source="user_config",
        )


def user_code_load_config():
    config.set_values(AutoloadedConfig().get_autoloaded_config(), "user_config")
