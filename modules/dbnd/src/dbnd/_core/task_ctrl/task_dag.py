import logging
import re
import typing

from dbnd._core.errors import DatabandError, friendly_error
from dbnd._core.task.task import Task
from dbnd._core.task.task_mixin import _TaskCtrlMixin
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.utils.task_utils import _try_get_task_from_airflow_op, to_tasks
from dbnd._core.utils.traversing import flatten


if typing.TYPE_CHECKING:
    from typing import Collection, Set

logger = logging.getLogger(__name__)


def _task_list(task_or_task_list):
    try:
        task_list = list(task_or_task_list)
    except TypeError:
        task_list = [task_or_task_list]

    from targets import Target

    task_list_output = []
    for t in task_list:
        if isinstance(t, Target):
            t = t.task
        airflow_task = _try_get_task_from_airflow_op(t)
        if airflow_task:
            t = airflow_task
        if not (isinstance(t, _TaskCtrlMixin)):
            raise DatabandError(
                "Relationships can only be set between "
                "Databand Tasks; received {}".format(t.__class__.__name__)
            )
        task_list_output.append(t)
    return task_list_output


class _TaskDagNode(TaskSubCtrl):
    def __init__(self, task):
        super(_TaskDagNode, self).__init__(task)

        self._upstream_tasks = set()
        self._downstream_tasks = set()

    def initialize_dag_node(self):
        # connect to all required tasks
        upstream = flatten(to_tasks(self.ctrl.task_inputs))
        upstream = list(filter(None, upstream))

        # take care of orphant tasks
        for child in self.task.descendants.get_children():
            if not child.task_dag.downstream:
                # it means there is no other tasks that are waiting for this task
                # -> we add it to task upstream
                # other options would be child is required by Parent task,
                # but it's not added yet ( anyway we create unique list)
                upstream.append(child)

        upstream = set(upstream)
        for upstream_task in upstream:
            self.set_upstream(upstream_task)

    def set_global_upstream(self, task_or_task_list):
        # don't set to itself for now, leafs are good enough
        # self.set_upstream(task_or_task_list=task_or_task_list)

        from dbnd.tasks import DataSourceTask

        children = {
            c
            for c in self.task.descendants.get_children()
            if not isinstance(c, DataSourceTask)
        }
        # do it only for leafs
        if not children:
            self.set_upstream(task_or_task_list=task_or_task_list)
        # or find the most "young child" (most upstream) and run it for him only
        # it's an ugly hack :)

        # let find all upstream tasks and intersect it with other children list.
        # the one with zero intersection is root in upstream!

        for c in children:
            upstreams = c.ctrl.task_dag.subdag_tasks()
            if not children & upstreams:
                c.set_global_upstream(task_or_task_list=task_or_task_list)

    def _task_id_to_tasks(self, task_ids):  # type: (Collection[str])->Set[Task]
        return {self.get_task_by_task_id(task_id) for task_id in task_ids}

    @property
    def upstream_task_ids(self):  # type: ()->Set[str]
        """list of tasks directly upstream"""
        return self._upstream_tasks

    @property
    def upstream(self):  # type: ()->Set[Task]
        """list of tasks directly upstream"""
        return self._task_id_to_tasks(self._upstream_tasks)

    @property
    def downstream_task_ids(self):  # type: ()->Set[str]
        """list of tasks directly downstream"""
        return self._downstream_tasks

    @property
    def downstream(self):  # type: ()->Set[Task]
        """list of tasks directly downstream"""
        return self._task_id_to_tasks(self._downstream_tasks)

    def set_upstream(self, task_or_task_list):
        self.set_relatives(task_or_task_list, upstream=True)

    def has_upstream(self, task):
        return task.task_id in self._upstream_tasks

    def set_downstream(self, task_or_task_list):
        self.set_relatives(task_or_task_list, upstream=False)

    def _direction(self, upstream):
        return self._upstream_tasks if upstream else self._downstream_tasks

    def subdag_tasks(self, should_run_only=False):
        return self._get_all_tasks(upstream=True, should_run_only=should_run_only)

    def _get_all_tasks(self, upstream=False, should_run_only=False):
        seen = set()
        result = []
        to_process = [self.task]
        # should be iterative, we don't like recursive as we can have huge nesting
        while to_process:
            current = to_process.pop(0)
            seen.add(current.task_id)
            if should_run_only and not current.ctrl.should_run():
                continue
            t_dag = current.ctrl.task_dag
            result.append(current)
            for t_connected_task_id in t_dag._direction(upstream):
                if t_connected_task_id in seen:
                    continue
                connected_task = self.get_task_by_task_id(t_connected_task_id)
                if not connected_task:
                    raise DatabandError(
                        "Can't resolve task %s by it's id" % t_connected_task_id
                    )
                to_process.append(connected_task)

        return set(result)

    def set_relatives(self, task_or_task_list, upstream=False):
        task_list = _task_list(task_or_task_list)
        for task in task_list:
            task.ctrl.task_dag._set_relatives(self.task, upstream=not upstream)
            self._set_relatives(task, upstream=upstream)

    def _set_relatives(self, task, upstream=False):
        connected = self._direction(upstream)
        if task.task_id in connected:
            connected_task = self.get_task_by_task_id(task.task_id)
            if id(task) == id(connected_task):
                logger.warning(
                    "Dependency {task} already registered for {current}"
                    "".format(task=task, current=self.task)
                )
            else:
                logger.debug("Re adding new implementation %s to %s", task, self.task)
        else:
            connected.add(task.task_id)

    def topological_sort(self):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """

        tasks = self.subdag_tasks()
        return topological_sort(tasks)

    def select_by_task_names(self, tasks_regexes, tasks=None):
        tasks = tasks or self.subdag_tasks()
        selected = []
        for task_regex in tasks_regexes:
            for t in tasks:
                if re.findall(task_regex, t.task_id):
                    selected.append(t)
        if not selected:
            raise friendly_error.no_matching_tasks_in_pipeline(
                tasks=tasks, tasks_regexes=tasks_regexes
            )
        return selected


def topological_sort(tasks, root_task=None):
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
            raise friendly_error.graph.cyclic_graph_detected(root_task, graph_unsorted)

    return tuple(graph_sorted)


def all_subdags(tasks):
    result = set()
    for sub_root in tasks:
        result = result.union(set(sub_root.task_dag.subdag_tasks()))
    return result
