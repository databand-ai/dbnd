# -*- coding: utf-8 -*-
# © Copyright Databand.ai, an IBM Company 2022

"""This package is deprecated! Please use the dbnd package instead!"""

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
