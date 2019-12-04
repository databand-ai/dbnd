import typing

from typing import Any, Dict, Tuple, Type

from dbnd._core.current import get_databand_run
from dbnd._core.decorator.task_decorator_spec import args_to_kwargs
from dbnd._core.task_run.task_run import TaskRun
from targets.inline_target import InlineTarget


if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task


def run_dynamic_task(parent_task_run, task_cls, call_args, call_kwargs):
    # type: (TaskRun, Type[Task], Tuple[Any], Dict[str,Any]) -> TaskRun
    from dbnd import pipeline, PipelineTask
    from dbnd._core.decorator.func_task_decorator import _default_output

    parent_task = parent_task_run.task
    dbnd_run = get_databand_run()

    if task_cls._conf__decorator_spec is not None:
        # orig_call_args, orig_call_kwargs = call_args, call_kwargs
        call_args, call_kwargs = args_to_kwargs(
            task_cls._conf__decorator_spec.args, call_args, call_kwargs
        )

    # Map all kwargs to the "original" target of that objects
    # for example: for DataFrame we'll try to find a relevant target that were used to read it
    # get all possible value's targets
    call_kwargs_as_targets = dbnd_run.target_origin.get_for_map(call_kwargs)
    for p_name, value_origin in call_kwargs_as_targets.items():
        call_kwargs[p_name] = InlineTarget(
            root_target=value_origin.origin_target,
            obj=call_kwargs[p_name],
            value_type=value_origin.value_type,
            source=value_origin.origin_target.source,
        )

    call_kwargs.setdefault("task_is_dynamic", True)
    call_kwargs.setdefault(
        "task_in_memory_outputs", parent_task.settings.dynamic_task.in_memory_outputs
    )

    # in case of pipeline - we'd like to run it as regular task
    # if False and issubclass(task_cls, PipelineTask):
    #     # TODO: do we want to support this behavior
    #     task_cls = task(task_cls._conf__decorator_spec.item).task_cls

    if issubclass(task_cls, PipelineTask):
        # if it's pipeline - create new databand run
        # create override _task_default_result to be object instead of target
        task_cls = pipeline(
            task_cls._conf__decorator_spec.item, _task_default_result=_default_output
        ).task_cls

        # instantiate inline pipeline
        t = task_cls(*call_args, **call_kwargs)
        run = dbnd_run.context.dbnd_run_task(t)
        return run.get_task_run(t.task_id)
    else:
        # instantiate inline task
        t = task_cls(*call_args, **call_kwargs)

        # update upstream/downstream relations - needed for correct tracking
        # we can have the task as upstream , as it was executed already
        if not parent_task.task_dag.has_upstream(t):
            parent_task.set_upstream(t)
        return dbnd_run.run_dynamic_task(t, task_engine=parent_task_run.task_engine)
