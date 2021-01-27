import logging
import typing

from typing import List

from dbnd._core.errors import DatabandRuntimeError
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


try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
except ImportError:
    # we are python2
    from dbnd._vendor.futures import ThreadPoolExecutor, as_completed


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task.task import Task

logger = logging.getLogger(__name__)


def check_if_completed_dfs(task, existing_completed_status=None):
    completed_status = existing_completed_status or dict()
    task_id = task.task_id
    completed_status[task_id] = completed = task._complete()
    if not completed:
        for c in task.ctrl.task_dag.upstream:
            if c.task_id in completed_status:
                continue
            completed_status.update(check_if_completed_dfs(c, completed_status))
    return completed_status


def check_if_completed_bfs(root_task, number_of_threads):
    completed_status = {}
    tasks_to_check_list = [root_task]

    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        while tasks_to_check_list:
            new_task_to_check_list = []

            task_results = {}
            for task in tasks_to_check_list:
                task_results[executor.submit(task._complete)] = task.task_id

            for future in as_completed(task_results):
                task_id = task_results[future]
                try:
                    data = future.result()
                    completed_status[task_id] = data
                except Exception as e:
                    raise DatabandRuntimeError(
                        "Failed to get completeness result of task_id {}".format(
                            task_id
                        ),
                        nested_exceptions=e,
                    )

            for task in tasks_to_check_list:
                if completed_status[task.task_id]:
                    continue

                for upstream_task in task.ctrl.task_dag.upstream:
                    if upstream_task.task_id not in completed_status:
                        new_task_to_check_list.append(upstream_task)

            tasks_to_check_list = new_task_to_check_list

    return completed_status


def find_tasks_to_skip_complete(root_tasks, all_tasks, number_of_threads):
    # if True = should run, if False or None - should notcheck_if_completed
    completed_status = {}

    logger.info("Looking for completed tasks..")

    if number_of_threads > 1:
        for t in root_tasks:
            completed_status.update(check_if_completed_bfs(t, number_of_threads))
    else:
        for t in root_tasks:
            completed_status.update(check_if_completed_dfs(t))

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

    def build_task_runs(self, run, root_task, task_run_engine):
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
        if run_config.selected_tasks_regex:
            task_dag = root_task.ctrl.task_dag  # type: _TaskDagNode
            roots = task_dag.select_by_task_names(
                run_config.selected_tasks_regex, tasks=tasks_to_run
            )

            tasks_skipped = tasks_to_run.difference(all_subdags(roots))

        enabled_tasks = tasks_to_run.difference(tasks_skipped)

        tasks_completed = set()
        task_skipped_as_not_required = set()
        if run_config.skip_completed:
            tasks_completed, task_skipped_as_not_required = find_tasks_to_skip_complete(
                roots, enabled_tasks, run_config.task_complete_parallelism_level
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
                # we want to have engine configuration with task overrides
                task_engine = build_task_from_config(
                    task_name=task_run_engine.task_name
                )
                task_engine.require_submit = task_run_engine.require_submit

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
