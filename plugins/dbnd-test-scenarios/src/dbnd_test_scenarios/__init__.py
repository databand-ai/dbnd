# © Copyright Databand.ai, an IBM Company 2022

from dbnd import relative_path
from dbnd._core.utils.basics.path_utils import abs_join
from targets import target


_scenarios_path = relative_path(
    __file__, "..", "..", "..", "..", "modules/dbnd/test_dbnd/scenarios"
)


def scenario_path(*path):
    return abs_join(_scenarios_path, *path)


def scenario_target(*path):
    return target(scenario_path(*path))
