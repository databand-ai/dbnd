from collections import namedtuple

from dbnd._core.current import get_databand_context
from dbnd._core.task_build.task_context import has_current_task


CONF_TASK_ENV_SECTION = "task_env"
CONF_TASK_SECTION = "task"
CONF_CONFIG_SECTION = "config"

ConfigPath = namedtuple("ConfigPath", ["section", "key"])


def from_task_env(key):
    return ConfigPath(CONF_TASK_ENV_SECTION, key)


def from_current_env(key):
    if has_current_task():
        return from_task_env(key)

    return ConfigPath(get_databand_context().env.task_name, key)
