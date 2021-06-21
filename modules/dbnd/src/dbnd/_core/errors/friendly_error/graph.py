import logging

from dbnd._core.errors import DatabandBuildError
from dbnd._core.errors.friendly_error.helpers import _band_call_str


logger = logging.getLogger()


def _find_cirlce(cyclic_nodes):
    # we are trying to find "real" circle our of this graph
    # so we traverse the graph till we find the "back" loop
    # and print only loop part of the graph
    #  A -> B -> C-> D ->..(B)
    #  we will print only  B->C->D..
    graph_unsorted = list(cyclic_nodes)
    # randomly select first node
    node = graph_unsorted[0]
    circle_trail = []

    while graph_unsorted:
        graph_unsorted.remove(node)
        circle_trail.append(node)
        for edge in node.ctrl.task_dag.upstream:
            if edge in circle_trail:
                # we found the circle, let return only relevant nodes
                return circle_trail[circle_trail.index(edge) :]

            # let just keep looking for the trail by continuing into the graph
            if edge in graph_unsorted:
                next_node = edge
                break
        else:
            next_node = graph_unsorted[0]
        node = next_node

    # we should not get here..
    return circle_trail


def cyclic_graph_detected(task, cyclic_nodes):
    # print the line of tasks
    circle_trail = _find_cirlce(cyclic_nodes)
    from dbnd._core.task_ctrl.task_dag_describe import tasks_trail

    logger.warning("Cyclic graph detected: %s", tasks_trail(circle_trail))
    return DatabandBuildError(
        "A cyclic dependency occurred %s: {short_trail}.. ({num_of_tasks} tasks)".format(
            task_call="in '{task_call}'" % _band_call_str(task) if task else "",
            short_trail=tasks_trail(circle_trail[:3]),
            num_of_tasks=len(circle_trail),
        ),
        show_exc_info=False,
        help_msg="Check your %s logic, see the full circle path in log. "
        "For better visability, use run.recheck_circle_dependencies=True"
        % _band_call_str(task),
    )
