# inline conftest

from dbnd import dbnd_config, relative_path


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]

dbnd_config.set_from_config_file(relative_path(__file__, "databand-test.cfg"))
