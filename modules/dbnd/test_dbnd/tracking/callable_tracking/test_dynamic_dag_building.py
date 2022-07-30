# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd import task
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import build_graph_from_calls


@task(result=("first", "extend"))
def first_step(fist_input):
    return "first_output", "extend_output"


@task
def second_step(second_input):
    return "second_output"


@task
def third_step(third_input, extend_input):
    return "third_output"


@task
def main_func():
    #
    #              --> second_step --> third_step
    # first_step -|                |
    #              ----------------
    #
    a, b = first_step("input")
    c = second_step(a)
    return third_step(c, b)


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
            {
                ("first_step", "second_step"),
                ("second_step", "third_step"),
                ("first_step", "third_step"),
            }
        )
