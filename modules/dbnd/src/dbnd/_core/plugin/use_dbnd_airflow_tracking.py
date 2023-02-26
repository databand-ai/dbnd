# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.errors import friendly_error


_AIRFLOW_PACKAGE_INSTALLED = None  # apache airflow is installed
_AIRFLOW_ENABLED = None  # dbnd-airflow is installed and enabled


def _is_airflow_enabled():
    if get_dbnd_project_config().is_no_modules:
        return False

    # TODO: make decision based on plugin only
    try:
        import dbnd_airflow  # noqa: F401

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


def should_use_airflow_monitor():
    if is_airflow_enabled():
        from dbnd_airflow.tracking.config import AirflowTrackingConfig

        tracking_config = AirflowTrackingConfig.from_databand_context()
        return tracking_config.af_with_monitor

    return True


def assert_airflow_enabled():
    if not is_airflow_enabled():
        raise friendly_error.config.missing_module("dbnd-airflow")
    return True


def assert_airflow_package_installed():
    global _AIRFLOW_PACKAGE_INSTALLED
    if _AIRFLOW_PACKAGE_INSTALLED is None:
        try:
            import airflow  # noqa: F401

            _AIRFLOW_PACKAGE_INSTALLED = True
        except Exception:
            _AIRFLOW_PACKAGE_INSTALLED = False
    return _AIRFLOW_PACKAGE_INSTALLED
