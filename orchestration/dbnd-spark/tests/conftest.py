# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]

skip_require_java_build = pytest.mark.skip("Requires JAVA project")


def pytest_configure(config):
    add_test_configuration(__file__)
