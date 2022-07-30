# © Copyright Databand.ai, an IBM Company 2022

from dbnd import relative_path
from dbnd._core.utils.project.project_fs import abs_join


_airflow_scenarios_default = relative_path(__file__)


def dbnd_airflow_test_scenarios_path(*path):
    return abs_join(_airflow_scenarios_default, *path)
