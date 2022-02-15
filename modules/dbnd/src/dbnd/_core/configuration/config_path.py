from collections import namedtuple


CONF_TASK_SECTION = "task"
CONF_CONFIG_SECTION = "config"

ConfigPath = namedtuple("ConfigPath", ["section", "key"])
