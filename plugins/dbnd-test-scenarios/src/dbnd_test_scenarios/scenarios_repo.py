import logging
import os

from dbnd._core.utils.project.project_fs import relative_path
from targets import target


logger = logging.getLogger(__name__)


def test_scenario_path(*path):
    scenarios_dir = relative_path(__file__, "..", "..", "scenarios")
    # if env var exists - use it as the examples dir, otherwise, calculate relative from here.

    return os.path.join(scenarios_dir, *path)


def test_scenario_target(*path):
    return target(test_scenario_path(*path))


class Scenarios(object):
    pass


scenarios = Scenarios()
