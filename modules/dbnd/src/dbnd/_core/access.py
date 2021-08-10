from typing import Any, Dict, Optional, Type, Union

import six

from dbnd._core.current import try_get_databand_run
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.task.base_task import _BaseTask


def get_remote_engine_name():
    # type: () -> Optional[str]
    """
    Retrieve the name of the remote engine of the current run if exists, None otherwise.
    """
    run = try_get_databand_run()
    if run:
        return run.run_executor.remote_engine.task_name


def get_task_params_values(task):
    # type: (_BaseTask) -> Dict[str, Any]
    """
    Access the task's user parameters values as a map between the name of the param and its current value
    """
    return {
        param.name: param.value
        for param in task.task_params.get_param_values(ParameterFilters.USER)
    }


def get_task_params_defs(task):
    # type: (Union[_BaseTask, Type[_BaseTask]]) -> Dict[str, ParameterDefinition]
    """
    Access the task's parameters definitions as a map between the name of the param and the parameter definition
    working with both task and task_class
    """
    return {
        k: v
        for k, v in six.iteritems(task.task_definition.task_param_defs)
        if not v.system
    }
