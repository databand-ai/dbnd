# © Copyright Databand.ai, an IBM Company 2022

import typing

from typing import Optional


if typing.TYPE_CHECKING:
    from dbnd_run.run_executor.run_executor import RunExecutor


def get_run_executor():
    # type: () -> RunExecutor
    """Returns current Task/Pipeline/Flow instance."""
    from dbnd_run.run_executor.run_executor import RunExecutor as _RunExecutor

    v = _RunExecutor.get_instance()
    return v


def try_get_run_executor():
    # type: () -> Optional[RunExecutor]
    from dbnd_run.run_executor.run_executor import RunExecutor as _RunExecutor

    if _RunExecutor.has_instance():
        return get_run_executor()
    return None


def is_killed():
    run_executor = try_get_run_executor()
    return run_executor and run_executor.is_killed()


def cancel_current_run(message=None):
    """Kills a run's execution from within the execution."""
    run_executor = try_get_run_executor()
    return run_executor.kill_run(message)
