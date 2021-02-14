from typing import Iterable, List, Set, Tuple

import pytest

from dbnd import task
from dbnd.api.tracking_api import TaskRunsInfo
from dbnd.testing.helpers_mocks import set_tracking_context


@task
def first_step(fist_input):
    return "first_output"


@task
def second_step(second_input):
    return "second_output"


@task
def third_step(third_input):
    return "third_output"


@task
def main_func():
    a = first_step("input")
    b = second_step(a)
    return third_step(b)


def build_graph_from_calls(mock_channel_tracker):
    multiple_task_runs_info = get_task_runs_info(mock_channel_tracker)
    return build_tasks_connection_from_runs_info(multiple_task_runs_info)


def build_tasks_connection_from_runs_info(multiple_task_runs_info):
    # type: (Iterable[TaskRunsInfo]) -> Tuple[Set[Tuple[str, str]],Set[Tuple[str, str]]]
    task_id_to_name = {}
    child_connections = set()
    downstream_connections = set()
    for task_runs_info in multiple_task_runs_info:
        for task_run in task_runs_info.task_runs:
            task_id_to_name[task_run.task_run_uid] = task_run.name

        for task_id, upstream_task_id in task_runs_info.upstreams_map:
            downstream_connections.add(
                (task_id_to_name[upstream_task_id], task_id_to_name[task_id])
            )

        for task_id, child in task_runs_info.parent_child_map:
            child_connections.add((task_id_to_name[task_id], task_id_to_name[child]))

    return child_connections, downstream_connections


def get_task_runs_info(mock_channel_tracker):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0].__name__ == "init_run":
            yield call[1]["init_args"].task_runs_info
        elif call.args[0].__name__ == "add_task_runs":
            yield call[1]["task_runs_info"]


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestDynamicDagBuilding(object):
    def test_building_connections_right(self, mock_channel_tracker):
        main_func()

        child_connections, downstream_connections = build_graph_from_calls(
            mock_channel_tracker
        )

        assert child_connections.issuperset(
            {
                ("main_func", "second_step"),
                ("main_func", "third_step"),
                ("main_func", "first_step"),
            }
        )

        assert downstream_connections.issuperset(
            {("first_step", "second_step"), ("second_step", "third_step")}
        )
