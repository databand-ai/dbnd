from collections import namedtuple

from dbnd._core.current import get_databand_context
from dbnd._core.task_build.task_context import has_current_task


CONF_TASK_SECTION = "task"
CONF_CONFIG_SECTION = "config"

ConfigPath = namedtuple("ConfigPath", ["section", "key"])
