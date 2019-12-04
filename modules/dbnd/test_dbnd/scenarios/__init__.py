import os

from dbnd._core.utils.project.project_fs import relative_path
from targets import target


def scenario_path(*path):
    return relative_path(__file__, *path)


def scenario_target(*path):
    return target(scenario_path(*path))
