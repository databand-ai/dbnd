import logging
import os

from dbnd._core.errors import friendly_error
from dbnd._core.plugin import dbnd_plugin_spec
from dbnd._core.utils.basics.load_python_module import _load_module
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


def assert_plugin_enabled(module, reason=None):
    if not is_plugin_enabled(module):
        raise friendly_error.config.missing_module(module, reason)
    return True


def is_web_enabled():
    return is_plugin_enabled("dbnd-web")


def assert_web_enabled(reason=None):
    return assert_plugin_enabled("dbnd-web", reason)


_dbnd_plugins_registered = False


def register_dbnd_plugins():
    if "DBND_NO_MODULES" in os.environ:
        return

    global _dbnd_plugins_registered
    if _dbnd_plugins_registered:
        return
    _dbnd_plugins_registered = True

    pm.load_setuptools_entrypoints("dbnd")
    pm.check_pending()


def register_dbnd_user_plugins(user_plugin_modules):
    for plugin_module in user_plugin_modules:
        pm.register(_load_module(plugin_module, "plugin:%s" % plugin_module))
