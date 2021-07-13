# -*- coding: utf-8 -*-

from dbnd import (
    Config,
    DataSourceTask,
    PipelineTask,
    PythonTask,
    Task,
    as_task,
    auto_namespace,
    band,
    config,
    config_deco,
    current,
    data,
    dbnd_context,
    dbnd_handle_errors,
    dbnd_tracking_start,
    dbnd_tracking_stop,
    hookimpl,
    namespace,
    new_dbnd_context,
    output,
    override,
    parameter,
    pipeline,
    task,
)
from dbnd.tasks import basics
from targets import _set_patches


__all__ = [
    "override",
    "config",
    "config_deco",
    "new_dbnd_context",
    "dbnd_context",
    "band",
    "pipeline",
    "task",
    "dbnd_handle_errors",
    "hookimpl",
    "dbnd_tracking_start",
    "dbnd_tracking_stop",
    "Config",
    "DataSourceTask",
    "PipelineTask",
    "PythonTask",
    "Task",
    "current",
    "auto_namespace",
    "namespace",
    "as_task",
    "data",
    "output",
    "parameter",
    "basics",
    "_set_patches",
]

dbnd_config = config
# if you need advanced Tasks or Parameters please use
# databand.tasks

# shortcuts for useful objects
str(_set_patches)  # NOQA

__version__ = "0.44.2"

__title__ = "databand"
__description__ = "Machine Learning Orchestration"
__url__ = "http://www.databand.ai/"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Evgeny Shulman"
__email__ = "evgeny.shulman@databand.ai"

__license__ = "Commercial Licenc e"
__copyright__ = "Copyright (c) 2018 databand.ai"
