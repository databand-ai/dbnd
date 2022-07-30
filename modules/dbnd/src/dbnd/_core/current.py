# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from typing import Optional

from dbnd._core.log import dbnd_log


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.settings import DatabandSettings
    from dbnd._core.task import Task
    from dbnd._core.task_run.task_run import TaskRun


def get_databand_context():
    # type: () -> DatabandContext
    """Databand Context get or create instance."""
    from dbnd._core.context.databand_context import DatabandContext as _DatabandContext

    return _DatabandContext.try_instance()


def try_get_databand_context():
    # type: () -> Optional[DatabandContext]

    from dbnd._core.context.databand_context import DatabandContext as _DatabandContext

    if not _DatabandContext.has_instance():
        return None
    return get_databand_context()


def dbnd_context():
    """Get or create databand context."""
    context = try_get_databand_context()
    if not context:
        # we are running without Databand Context
        # let create one inplace
        from dbnd._core.context.databand_context import (
            DatabandContext as _DatabandContext,
        )

        context = _DatabandContext.try_instance(name="inplace_run")
    return context


def get_databand_run():
    # type: () -> DatabandRun
    """Returns current Task/Pipeline/Flow instance."""
    from dbnd._core.run.databand_run import DatabandRun as _DatabandRun

    v = _DatabandRun.get_instance()
    return v


def try_get_databand_run():
    # type: () -> Optional[DatabandRun]
    from dbnd._core.run.databand_run import DatabandRun as _DatabandRun

    if _DatabandRun.has_instance():
        return get_databand_run()
    return None


def in_tracking_run():
    # type: () -> bool
    run = try_get_databand_run()
    if run:
        return not run.is_orchestration

    return False


def is_orchestration_run():
    # type: () -> bool
    run = try_get_databand_run()
    if run:
        return run.is_orchestration

    return False


def current_task():
    # type: () -> Task
    """
    Returns the current task's object.

    Example::

        from dbnd import current_task, task

        @task
        def calculate_alpha(alpha: int = 0.5):
            return current_task().task_version
    """
    from dbnd._core.task_build.task_context import current_task as ct

    return ct()


def try_get_current_task():
    from dbnd._core.task_build.task_context import try_get_current_task as tgct

    return tgct()


def get_task_by_task_id(task_id):
    # type: (str) -> Task
    return get_databand_context().task_instance_cache.get_task_by_id(task_id)


def try_get_current_task_run():
    # type: () -> TaskRun
    run = try_get_databand_run()
    if not run:
        return None
    task = try_get_current_task()
    if task:
        return run.get_task_run(task.task_id)
    else:
        return None


def current_task_run():
    # type: () -> TaskRun
    """
    Returns the current task run.

    Example::

        from dbnd import current_task_run, task

        @task
        def calculate_alpha(alpha: int = 0.5):
            return current_task_run()
    """
    return get_databand_run().get_task_run(current_task().task_id)


def get_settings():
    # type: () -> DatabandSettings
    v = get_databand_context().settings  # type: DatabandSettings
    return v


def is_verbose():
    return dbnd_log.is_verbose()


def get_target_logging_level():
    default_level = 10  # DEBUG

    context = try_get_databand_context()
    if context and getattr(context, "settings", None):
        return getattr(logging, context.settings.log.targets_log_level)

    return default_level


def is_killed():
    run = try_get_databand_run()
    return run and run.is_killed()


def cancel_current_run(message=None):
    """Kills a run's execution from within the execution."""
    current_databand_run = get_databand_run()
    return current_databand_run.kill_run(message)
