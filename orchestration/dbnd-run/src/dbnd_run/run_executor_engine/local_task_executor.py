# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.constants import TaskRunState
from dbnd._core.errors.base import DatabandRunError, DatabandSigTermError
from dbnd_run import errors
from dbnd_run.run_executor_engine import RunExecutorEngine


logger = logging.getLogger(__name__)


class LocalTaskExecutor(RunExecutorEngine):
    def do_run(self):
        topological_tasks = topological_sort([tr.task for tr in self.task_runs])
        fail_fast = self.run_executor.run_config.fail_fast
        task_failed = False

        task_runs_to_update_state = []
        for task in topological_tasks:
            tr = self.run.get_task_run_by_id(task.task_id)
            if tr.is_reused:
                continue

            if fail_fast and task_failed:
                state = self.run_executor.get_upstream_failed_task_run_state(tr)

                logger.info("Setting %s to %s", task.task_id, state)
                tr.set_task_run_state(state, track=False)
                task_runs_to_update_state.append(tr)
                continue

            upstream_task_runs = [
                self.run.get_task_run_by_id(t.task_id)
                for t in task.ctrl.task_dag.upstream
            ]
            failed_upstream = [
                upstream_task_run
                for upstream_task_run in upstream_task_runs
                if upstream_task_run.task_run_state in TaskRunState.fail_states()
            ]
            if failed_upstream:
                logger.info(
                    "Setting %s to %s", task.task_id, TaskRunState.UPSTREAM_FAILED
                )
                tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED, track=False)
                task_runs_to_update_state.append(tr)
                continue

            if self.run_executor.is_killed():
                logger.info(
                    "Databand Context is killed! Stopping %s to %s",
                    task.task_id,
                    TaskRunState.FAILED,
                )
                tr.set_task_run_state(TaskRunState.FAILED, track=False)
                task_runs_to_update_state.append(tr)
                continue

            logger.debug("Executing task: %s", task.task_id)

            try:
                tr.task_run_executor.execute()
            except DatabandSigTermError as e:
                raise e
            except Exception:
                task_failed = True
                logger.exception("Failed to execute task '%s':" % task.task_id)

        if task_runs_to_update_state:
            self.run.tracker.set_task_run_states(task_runs_to_update_state)

        if task_failed:
            err = _collect_errors(self.run.task_runs)

            if err:
                raise DatabandRunError(err)


def _collect_errors(task_runs):
    err = ""
    upstream_failed = []
    failed = []
    for task_run in task_runs:
        task_name = task_run.task.task_name
        if task_run.task_run_state == TaskRunState.UPSTREAM_FAILED:
            # we don't want to show upstream failed in the list
            upstream_failed.append(task_name)
        elif task_run.task_run_state in TaskRunState.direct_fail_states():
            failed.append(task_name)
    if upstream_failed:
        err += "Task that didn't run because of failed dependency:\n\t{}\n".format(
            "\n\t".join(upstream_failed)
        )
    if failed:
        err += "Failed tasks are:\n\t{}".format("\n\t".join(failed))
    return err


def topological_sort(tasks, root_task=None):
    """
    Sorts tasks in topographical order, such that a task comes after any of its
    upstream dependencies.

    Heavily inspired by:
    http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

    :return: list of tasks in topological order
    """

    # special case
    if len(tasks) == 0:
        return tuple()

    graph_unsorted = set(tasks)

    graph_sorted = []

    # Run until the unsorted graph is empty.
    while graph_unsorted:
        # Go through each of the node/edges pairs in the unsorted
        # graph. If a set of edges doesn't contain any nodes that
        # haven't been resolved, that is, that are still in the
        # unsorted graph, remove the pair from the unsorted graph,
        # and append it to the sorted graph. Note here that by using
        # using the items() method for iterating, a copy of the
        # unsorted graph is used, allowing us to modify the unsorted
        # graph as we move through it. We also keep a flag for
        # checking that that graph is acyclic, which is true if any
        # nodes are resolved during each pass through the graph. If
        # not, we need to bail out as the graph therefore can't be
        # sorted.
        acyclic = False
        for node in list(graph_unsorted):
            for edge in node.ctrl.task_dag.upstream:
                if edge in graph_unsorted:
                    break
            # no edges in upstream tasks
            else:
                # we found at least one node in graph
                acyclic = True
                graph_unsorted.remove(node)
                graph_sorted.append(node)

        if not acyclic:
            raise errors.graph.cyclic_graph_detected(root_task, graph_unsorted)

    return tuple(graph_sorted)
