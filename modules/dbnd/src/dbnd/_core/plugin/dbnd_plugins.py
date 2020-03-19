import importlib
import logging

from dbnd._core.configuration import environ_config
from dbnd._core.errors import friendly_error
from dbnd._core.plugin import dbnd_plugin_spec
from dbnd._core.utils.seven import import_errors
from dbnd._vendor import pluggy


logger = logging.getLogger(__name__)


hookimpl = pluggy.HookimplMarker("dbnd")
pm = pluggy.PluginManager("dbnd")
pm.add_hookspecs(dbnd_plugin_spec)

_AIRFLOW_PACKAGE_INSTALLED = None  # apache airflow is installed
_AIRFLOW_ENABLED = None  # dbnd-airflow is installed and enabled


def _is_airflow_enabled():
    if pm.has_plugin("dbnd-airflow"):
        return True

    if environ_config.is_no_modules():
        return False

    # TODO: make decision based on plugin only
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


def disable_airflow_plugin():
    global _AIRFLOW_ENABLED
    _AIRFLOW_ENABLED = False
    pm.set_blocked("dbnd-airflow")


def use_airflow_connections():
    from dbnd._core.configuration.dbnd_config import config

    return is_airflow_enabled() and config.getboolean("airflow", "use_connections")


def assert_airflow_enabled():
    if not is_airflow_enabled():
        raise friendly_error.config.missing_module("dbnd-airflow")
    return True


def assert_airflow_package_installed():
    global _AIRFLOW_PACKAGE_INSTALLED
    if _AIRFLOW_PACKAGE_INSTALLED is None:
        try:
            import airflow

            _AIRFLOW_PACKAGE_INSTALLED = True
        except Exception:
            _AIRFLOW_PACKAGE_INSTALLED = False
    return _AIRFLOW_PACKAGE_INSTALLED


# all other modules
def is_plugin_enabled(module, module_import=None):
    if pm.has_plugin(module):
        return True

    if module_import:
        try:
            importlib.import_module(module_import)
            return True
        except import_errors:
            pass

    return False


def assert_plugin_enabled(module, reason=None, module_import=None):
    if not is_plugin_enabled(module, module_import=module_import):
        raise friendly_error.config.missing_module(module, reason)
    return True


def is_web_enabled():
    return is_plugin_enabled("dbnd-web", "dbnd_web")


def assert_web_enabled(reason=None):
    return assert_plugin_enabled("dbnd-web", reason=reason, module_import="dbnd_web")
