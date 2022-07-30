# © Copyright Databand.ai, an IBM Company 2022

import logging

from collections import Counter, defaultdict
from functools import partial
from typing import Any, Optional, Union

import six

from dbnd._core.errors import DatabandSystemError, friendly_error
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.task.task import Task
from dbnd._core.utils.traversing import traverse
from targets.base_target import Target
from targets.multi_target import MultiTarget
from targets.target_factory import target
from targets.types import Path


logger = logging.getLogger(__name__)


def _try_get_task_from_airflow_op(value):
    if is_airflow_enabled():
        from dbnd_airflow.dbnd_task_executor.converters import try_operator_to_dbnd_task

        return try_operator_to_dbnd_task(value)


def _to_task(value):
    if isinstance(value, Task):
        return value

    if isinstance(value, Target):
        if value.source_task:
            return value.source_task
        if isinstance(value, MultiTarget):
            tasks = [_to_task(t) for t in value.targets]
            return [t for t in tasks if t is not None]

        # we are not going to create "task" for the target,
        # if it doesn't have an source
        return None

    airflow_op_task = _try_get_task_from_airflow_op(value)
    if airflow_op_task:
        return airflow_op_task

    logger.debug("Can't convert '%s' to task.", value)
    return None


def to_tasks(obj_or_struct):
    return traverse(obj_or_struct, convert_f=_to_task, filter_none=True)


def _to_target(value, from_string_kwargs=None):
    if value is None:
        return value

    if isinstance(value, Target):
        return value

    if isinstance(value, Task):
        if value.task_definition.single_result_output:
            return value.result
        return value.task_outputs

    airflow_op_task = _try_get_task_from_airflow_op(value)
    if airflow_op_task:
        return airflow_op_task.task_outputs

    if isinstance(value, six.string_types):
        from_string_kwargs = from_string_kwargs or {}
        return target(value, **from_string_kwargs)

    if isinstance(value, Path):
        from_string_kwargs = from_string_kwargs or {}
        return target(str(value), **from_string_kwargs)

    raise friendly_error.failed_to_convert_value_to_target(value)


def to_targets(obj_or_struct, from_string_kwargs=None):
    return traverse(
        obj_or_struct,
        convert_f=partial(_to_target, from_string_kwargs=from_string_kwargs),
        filter_none=True,
    )


def targets_to_str(obj_or_struct):
    return traverse(obj_or_struct, convert_f=target_to_str, filter_none=True)


def target_to_str(obj):
    # type: (Any) -> str
    if isinstance(obj, six.string_types):
        return obj
    return str(obj)


def tasks_summary(tasks):
    if not tasks:
        return ""
    tasks_count = [
        "%s %s" % (count, key)
        for key, count in Counter((t.task_name for t in tasks)).most_common()
    ]
    return "{stats}  - total {total}".format(
        total=len(tasks), stats=",".join(tasks_count)
    )


def tasks_to_ids_set(tasks):
    return set(t.task_id for t in tasks)


def calculate_friendly_task_ids(tasks):
    tasks_by_name = defaultdict(list)
    for t in tasks:
        tasks_by_name[t.task_name].append(t)
    task_friendly_ids = {}
    for tasks in tasks_by_name.values():
        sorted_tasks = sorted(tasks, key=lambda t: t.task_creation_id)
        for i, task in enumerate(sorted_tasks):
            if i > 0 or len(sorted_tasks) > 1:
                task_af_id = "{}_{}".format(task.task_name, i)
            else:
                task_af_id = task.task_name
            task_friendly_ids[task.task_id] = task_af_id
    return task_friendly_ids


def get_task_name_safe(task_or_task_name):
    # type: (Union[Task, str]) -> str
    if task_or_task_name is None or isinstance(task_or_task_name, six.string_types):
        return task_or_task_name

    if isinstance(task_or_task_name, Task):
        return task_or_task_name.task_name
    raise DatabandSystemError(
        "Can't calculate task name from %s - %s",
        task_or_task_name,
        type(task_or_task_name),
    )


def get_project_name_safe(project_name, task_or_task_name):
    # type: (Union[Task, str], str) -> Optional[str]
    if project_name:
        return project_name

    if task_or_task_name is None or isinstance(task_or_task_name, six.string_types):
        return None

    if isinstance(task_or_task_name, Task) and hasattr(task_or_task_name, "project"):
        return task_or_task_name.project
    return None
