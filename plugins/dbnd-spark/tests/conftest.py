from dbnd._core.plugin.dbnd_plugins import disable_airflow_plugin


#
# disable_airflow_plugin()
pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]
