import importlib
import logging
import os

from dbnd._core.errors import friendly_error
from dbnd._core.plugin import dbnd_plugin_spec
from dbnd._core.utils.basics.load_python_module import _load_module, import_errors
from dbnd._vendor import pluggy


logger = logging.getLogger(__name__)


hookimpl = pluggy.HookimplMarker("dbnd")
pm = pluggy.PluginManager("dbnd")
pm.add_hookspecs(dbnd_plugin_spec)


_AIRFLOW_ENABLED = None


def _is_airflow_enabled():
    if pm.has_plugin("dbnd-airflow"):
        return True
    try:
        import dbnd_airflow

        return True
    except ImportError:
        return False


def is_airflow_enabled():
    global _AIRFLOW_ENABLED
    if _AIRFLOW_ENABLED is None:
        _AIRFLOW_ENABLED = _is_airflow_enabled()
    return _AIRFLOW_ENABLED


def assert_airflow_enabled():
    if not is_airflow_enabled():
        raise friendly_error.config.missing_module("dbnd-airflow")
    return True


# all other modules
def is_plugin_enabled(module):
    return pm.has_plugin(module)


def assert_plugin_enabled(module):
    if not is_plugin_enabled(module):
        raise friendly_error.config.missing_module(module)
    return True


def is_web_enabled():
    return is_plugin_enabled("dbnd-web")


def assert_web_enabled():
    return assert_plugin_enabled("dbnd-web")


def register_dbnd_plugins():
    if "DBND_NO_MODULES" in os.environ:
        return

    pm.load_setuptools_entrypoints("dbnd")

    # we want to support old "databand" module install that doesn't use plugins
    # so if we don't have plugins registered --> probably we in the  Fat Wheel mode
    # we need to register them manually
    dbnd_plugins = [
        name
        for name, p in pm.list_name_plugin()
        if name.startswith("dbnd-") and name != "dbnd-examples"
    ]
    if not dbnd_plugins:
        _register_manually()

    pm.check_pending()


def register_dbnd_user_plugins(user_plugin_modules):
    for plugin_module in user_plugin_modules:
        pm.register(_load_module(plugin_module, "plugin:%s" % plugin_module))


def _register_manually():
    # should be used when modules are installed not via setup.py
    # ( old packaging system)
    for plugin in [
        "dbnd-airflow",
        "dbnd-web",
        "dbnd-aws",
        "dbnd-azure",
        "dbnd-databricks",
        "dbnd-docker",
        "dbnd-hdfs",
        "dbnd-gcp",
        "dbnd-spark",
        "dbnd-examples",
    ]:

        plugin_module = "%s._plugin" % plugin.replace("-", "_")
        try:
            plugin_module = importlib.import_module(plugin_module)
        except import_errors:
            logger.info("Failed to import %s", plugin_module)
            continue

        try:
            pm.register(plugin_module, plugin)
        except Exception as ex:
            logger.error("failed to register %s:%s", plugin, ex)


# TODO Seems like not used
def _register_from_config():
    if specs is not None and not isinstance(specs, types.ModuleType):
        if isinstance(specs, str):
            specs = specs.split(",") if specs else []
        if not isinstance(specs, (list, tuple)):
            raise UsageError(
                "Plugin specs must be a ','-separated string or a "
                "list/tuple of strings for plugin names. Given: %r" % specs
            )
        return list(specs)
    return []
