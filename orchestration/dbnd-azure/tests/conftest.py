# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.configuration.environ_config import set_orchestration_mode
from dbnd.testing.test_config_setter import add_test_configuration


# inline conftest


pytest_plugins = [
    "dbnd.orchestration.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    set_orchestration_mode()
    add_test_configuration(__file__)
