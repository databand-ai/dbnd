# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.plugin.use_dbnd_airflow_tracking import is_airflow_enabled


def _is_dbnd_run_airflow_enabled():
    if get_dbnd_project_config().is_no_modules:
        return False

    try:
        import dbnd_run  # noqa: F401
    except ImportError:
        return False

    try:
        import airflow  # noqa: F401
    except ImportError:
        return False
    return True


_DBND_RUN_AIRFLOW_AIRFLOW_ENABLED = None  # dbnd-airflow is installed and enabled


def is_dbnd_run_airflow_enabled():
    global _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED
    if _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED is None:
        _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED = _is_dbnd_run_airflow_enabled()
    return _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED


def disable_dbnd_run_airflow_plugin():
    global _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED
    _DBND_RUN_AIRFLOW_AIRFLOW_ENABLED = False


def use_airflow_connections():
    from dbnd._core.configuration.dbnd_config import config

    return is_airflow_enabled() and config.getboolean("airflow", "use_connections")
