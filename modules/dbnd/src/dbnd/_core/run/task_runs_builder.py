import logging
import typing

from typing import List

from dbnd._core.settings import EngineConfig, RunConfig
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.task_ctrl.task_dag import _TaskDagNode, all_subdags
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.task_utils import (
    calculate_friendly_task_ids,
    tasks_summary,
    tasks_to_ids_set,
)


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task.task import Task

logger = logging.getLogger(__name__)


def find_tasks_to_skip_complete(root_tasks, all_tasks):
    completed_status = {}  # if True = should run, if False or None - should not

    logger.info("Looking for completed tasks..")

    def check_if_completed(task):
        task_id = task.task_id
        if task_id in completed_status:
            return
        completed_status[task_id] = completed = task._complete()
        if not completed:
            for c in task.ctrl.task_dag.upstream:
                check_if_completed(c)

    for t in root_tasks:
        check_if_completed(t)

    # only if completed_status is False task is not skipped
    # otherwise - it wasn't discovered or it's completed
    # it task is not discovered - we don't need to run it.

    skipped_tasks = {
        task for task in all_tasks if completed_status.get(task.task_id) is None
    }
    completed_tasks = {
        task for task in all_tasks if completed_status.get(task.task_id) is True
    }
    logger.info(
        "Found %s completed and %s skipped tasks",
        len(completed_tasks),
        len(skipped_tasks),
    )
    return completed_tasks, skipped_tasks


class TaskRunsBuilder(object):
    def get_tasks_without_disabled(self, task):
        td = task.ctrl.task_dag  # type: _TaskDagNode

        # we have to run twice as not runnable can have runnable upstream
        # TODO: validate that
        all_tasks = td.subdag_tasks()
        runnable_tasks = td.subdag_tasks(should_run_only=True)

        tasks_disabled = all_tasks.difference(runnable_tasks)

        return runnable_tasks, tasks_disabled

    def build_task_runs(self, run, root_task, remote_engine):
        # type: (DatabandRun, Task, EngineConfig) -> List[TaskRun]
        run_config = run.context.settings.run  # type: RunConfig

        # first, let remove all tasks explicitly marked as disabled by user
        tasks_to_run, tasks_disabled = self.get_tasks_without_disabled(root_task)
        if tasks_disabled:
            logger.info(
                "Tasks were removed from the task graph as they are marked as not to run: %s",
                tasks_summary(tasks_disabled),
            )

        roots = [root_task]
        tasks_skipped = set()
        # in case we need to run only part of the graph we mark all other tasks as skipped
        if run_config.task or run_config.id:
            task_dag = root_task.ctrl.task_dag  # type: _TaskDagNode
            if run_config.task:
                roots = task_dag.select_by_task_names(
                    run_config.task, tasks=tasks_to_run
                )
            elif run_config.id:
                roots = task_dag.select_by_task_ids(run_config.id, tasks=tasks_to_run)

            tasks_skipped = tasks_to_run.difference(all_subdags(roots))

        enabled_tasks = tasks_to_run.difference(tasks_skipped)

        tasks_completed = set()
        task_skipped_as_not_required = set()
        if run_config.skip_completed:
            tasks_completed, task_skipped_as_not_required = find_tasks_to_skip_complete(
                roots, enabled_tasks
            )

        # # if any of the tasks is spark add policy
        # from dbnd._core.task.spark import _BaseSparkTask
        # for t in tasks_to_run:
        #     if isinstance(t, _BaseSparkTask):
        #         t.spark.apply_spark_cluster_policy(t)
        # bash_op = BashOperator(task_id="echo", bash_command="echo hi")
        # self.root_task.set_upstream(bash_op.task)

        friendly_ids = calculate_friendly_task_ids(tasks_to_run)

        completed_ids = tasks_to_ids_set(tasks_completed)
        task_skipped_as_not_required_ids = tasks_to_ids_set(
            task_skipped_as_not_required
        )
        skipped_ids = tasks_to_ids_set(tasks_skipped)

        task_runs = []
        for task in tasks_to_run:

            with task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                # we want to have configuration with task overrides
                task_engine = build_task_from_config(task_name=remote_engine.task_name)
                task_engine.require_submit = remote_engine.require_submit
                task_af_id = friendly_ids[task.task_id]
                task_run = TaskRun(
                    task=task, run=run, task_af_id=task_af_id, task_engine=task_engine
                )
            if task.task_id in completed_ids:
                task_run.is_reused = True

            if task.task_id in task_skipped_as_not_required_ids:
                task_run.is_reused = True
                task_run.is_skipped_as_not_required = True

            if task.task_id in skipped_ids:
                task_run.is_skipped = True

            if task.task_id == root_task.task_id:
                task_run.is_root = True

            task_runs.append(task_run)

        return task_runs
