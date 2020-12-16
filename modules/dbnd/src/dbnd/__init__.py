# -*- coding: utf-8 -*-


from dbnd._core.cli.main import (
    dbnd_cmd,
    dbnd_run_cmd,
    dbnd_run_cmd_main,
    main as dbnd_main,
)
from dbnd._core.commands import log_dataframe, log_duration, log_metric, log_metrics
from dbnd._core.configuration.config_path import ConfigPath
from dbnd._core.configuration.config_readers import override
from dbnd._core.configuration.config_store import ConfigMergeSettings
from dbnd._core.configuration.dbnd_config import config, config_deco
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.current import (
    current_task,
    current_task_run,
    dbnd_context,
    get_databand_context,
    get_databand_run,
)
from dbnd._core.decorator.dbnd_decorator import band, pipeline, task
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.inplace_run.inplace_run_manager import dbnd_run_start, dbnd_run_stop
from dbnd._core.parameter.parameter_builder import data, output, parameter
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    ParameterScope,
)
from dbnd._core.plugin.dbnd_plugins import hookimpl
from dbnd._core.task.config import Config
from dbnd._core.task.data_source_task import DataSourceTask
from dbnd._core.task.pipeline_task import PipelineTask
from dbnd._core.task.python_task import PythonTask
from dbnd._core.task.task import Task
from dbnd._core.task_build import task_namespace
from dbnd._core.task_build.task_context import current
from dbnd._core.task_build.task_namespace import auto_namespace, namespace
from dbnd._core.task_build.task_registry import register_config_cls, register_task
from dbnd._core.task_ctrl.task_relations import as_task
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.tracking.python_tracking import (
    track_functions,
    track_module_functions,
    track_modules,
)
from dbnd._core.utils.project.project_fs import (
    databand_lib_path,
    databand_system_path,
    project_path,
    relative_path,
)
from dbnd.tasks import basics
from targets import _set_patches


from dbnd._core.configuration.environ_config import (  # isort:skip
    get_dbnd_project_config,
)


get_dbnd_project_config().validate_init()  # isort:skip


dbnd_config = config
__all__ = [
    "hookimpl",
    # context management
    "new_dbnd_context",
    "current",
    "dbnd_context",
    "current_task",
    "current_task_run",
    "get_databand_run",
    "get_databand_context",
    # inplace implementation
    "dbnd_run_start",
    "dbnd_run_stop",
    "auto_namespace",
    "namespace",
    "task_namespace",
    "as_task",
    # tasks
    "band",
    "pipeline",
    "task",
    "Task",
    # class tasks
    "Config",
    "DataSourceTask",
    "PipelineTask",
    "PythonTask",
    # parameters
    "parameter",
    "data",
    "output",
    # config
    "dbnd_config",
    "override",
    "config",
    "config_deco",
    "ConfigPath",
    "ConfigMergeSettings",
    "ParameterScope",
    "ParameterDefinition",
    # dbnd run
    "dbnd_main",
    "dbnd_cmd",
    "dbnd_run_cmd",
    "dbnd_run_cmd_main",
    "dbnd_handle_errors",
    # metrics
    "log_dataframe",
    "LogDataRequest",
    "log_metric",
    "log_metrics",
    "log_duration",
    # project paths
    "project_path",
    "relative_path",
    "databand_lib_path",
    "databand_system_path",
    # bootstrap
    "dbnd_bootstrap",
    "_set_patches",
    "track_modules",
    "track_module_functions",
    "track_functions",
]

# validate missing __all__
# imported_vars = set(k for k in locals().keys() if not k.startswith("__"))
# print(list(imported_vars.difference(set(__all__))))

# shortcuts for useful objects
str(_set_patches)  # NOQA
__version__ = "0.31.2"

__title__ = "databand"
__description__ = "Machine Learning Orchestration"
__url__ = "http://www.databand.ai/"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Evgeny Shulman"
__email__ = "evgeny.shulman@databand.ai"

__license__ = "Commercial Licenc e"
__copyright__ = "Copyright (c) 2018 databand.ai"
