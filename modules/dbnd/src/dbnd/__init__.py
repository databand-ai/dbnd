# -*- coding: utf-8 -*-
# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.access import (
    get_remote_engine_name,
    get_task_params_defs,
    get_task_params_values,
)
from dbnd._core.cli.main import dbnd_cmd, dbnd_run_cmd, main as dbnd_main
from dbnd._core.configuration.config_path import ConfigPath
from dbnd._core.configuration.config_store import replace_section_with
from dbnd._core.configuration.config_value import default, extend, override
from dbnd._core.configuration.dbnd_config import config, config_deco
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.context.use_dbnd_run import (
    is_dbnd_run_package_installed,
    set_orchestration_mode,
)
from dbnd._core.current import (
    current_task,
    current_task_run,
    dbnd_context,
    get_databand_context,
    get_databand_run,
)
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.log.dbnd_log import set_verbose
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.parameter.parameter_builder import data, output, parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.task.config import Config
from dbnd._core.task_build.dbnd_decorator import (
    band,
    data_source_pipeline,
    pipeline,
    task,
)
from dbnd._core.task_build.task_context import current
from dbnd._core.task_build.task_registry import register_config_cls, register_task
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.tracking.metrics import (
    dataset_op_logger,
    log_artifact,
    log_dataframe,
    log_dataset_op,
    log_duration,
    log_metric,
    log_metrics,
)
from dbnd._core.tracking.no_tracking import dont_track
from dbnd._core.tracking.python_tracking import (
    track_functions,
    track_module_functions,
    track_modules,
    track_scope_functions,
)
from dbnd._core.tracking.script_tracking_manager import (
    dbnd_tracking,
    dbnd_tracking_start,
    dbnd_tracking_stop,
)
from dbnd._core.utils.project.project_fs import (
    databand_lib_path,
    databand_system_path,
    project_path,
    relative_path,
)
from dbnd.providers.dbt.dbt_cloud import collect_data_from_dbt_cloud
from dbnd.providers.dbt.dbt_core import collect_data_from_dbt_core
from targets import _set_patches


from dbnd._core.configuration.environ_config import (  # isort:skip
    disable_dbnd,
    get_dbnd_project_config,
)

dbnd_config = config
dbnd_config.__doc__ = """Defines a dictionary of configuration settings that you can pass to your tasks."""
__all__ = [
    # context management
    "new_dbnd_context",
    "current",
    "dbnd_context",
    "current_task",
    "current_task_run",
    "get_databand_run",
    "get_databand_context",
    "set_verbose",
    "disable_dbnd",
    # inplace implementation
    "dbnd_tracking",
    "dbnd_tracking_start",
    "dbnd_tracking_stop",
    "register_config_cls",
    "register_task",
    "dont_track",
    # tasks
    "band",
    "pipeline",
    "data_source_pipeline",
    "task",
    # class tasks
    "Config",
    # parameters
    "parameter",
    "data",
    "output",
    # config
    "dbnd_config",
    "override",
    "extend",
    "config",
    "config_deco",
    "ConfigPath",
    "ParameterDefinition",
    "ParameterScope",
    "replace_section_with",
    "default",
    # metrics
    "log_dataframe",
    "dataset_op_logger",
    "LogDataRequest",
    "log_artifact",
    "log_metric",
    "log_metrics",
    "log_duration",
    "log_dataset_op",
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
    "track_scope_functions",
    # access helpers
    "get_task_params_defs",
    "get_task_params_values",
    "get_remote_engine_name",
    "collect_data_from_dbt_cloud",
    "collect_data_from_dbt_core",
    "dbnd_run_cmd",
    # -= Orchestration =-
    "set_orchestration_mode",
    # dbnd run cmds functions
    "dbnd_main",
    "dbnd_cmd",
    "dbnd_run_cmd",
    "dbnd_handle_errors",
]

if is_dbnd_run_package_installed():
    from dbnd_run.cli.cmd_run import dbnd_run_cmd_main  # noqa: F401
    from dbnd_run.plugin.dbnd_plugins import hookimpl  # noqa: F401
    from dbnd_run.task.data_source_task import DataSourceTask  # noqa: F401
    from dbnd_run.task.pipeline_task import PipelineTask  # noqa: F401
    from dbnd_run.task.python_task import PythonTask  # noqa: F401
    from dbnd_run.task.task import Task  # noqa: F401
    from dbnd_run.task_ctrl import task_namespace  # noqa: F401
    from dbnd_run.task_ctrl.task_namespace import (  # noqa: F401
        auto_namespace,
        namespace,
    )
    from dbnd_run.task_ctrl.task_relations import as_task  # noqa: F401

    __all__ += [
        "as_task",
        "basics",
        "dbnd_run_cmd_main",
        "hookimpl",
        # namespace implementation
        "auto_namespace",
        "namespace",
        "task_namespace",
        # class tasks
        "DataSourceTask",
        "Task",
        "PipelineTask",
        "PythonTask",
        ## Orchestration
        # dbnd run cmds functions
        "dbnd_run_cmd_main",
    ]

# validate missing __all__
# imported_vars = set(k for k in locals().keys() if not k.startswith("__"))
# print(list(imported_vars.difference(set(__all__))))

# shortcuts for useful objects
str(_set_patches)  # NOQA
__version__ = "1.0.23.2"

__title__ = "databand"
__description__ = "Machine Learning Orchestration"
__url__ = "http://www.databand.ai/"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Evgeny Shulman"
__email__ = "evgeny.shulman@databand.ai"

__license__ = "OSI Approved Apache Software License"
__copyright__ = "Copyright (c) 2018 databand.ai"
