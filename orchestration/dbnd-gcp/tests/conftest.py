# © Copyright Databand.ai, an IBM Company 2022
from dbnd import dbnd_bootstrap
from dbnd.testing.test_config_setter import add_test_configuration


# inline conftest


pytest_plugins = [
    "dbnd_run.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    dbnd_bootstrap(enable_dbnd_run=True)
    add_test_configuration(__file__)
