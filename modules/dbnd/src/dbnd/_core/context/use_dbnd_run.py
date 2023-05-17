# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.errors import friendly_error


_DBND_RUN_PACKAGE_INSTALLED = None  # dbnd-run is installed
_AIRFLOW_PACKAGE_INSTALLED = None  # apache airflow is installed


def is_dbnd_run_package_installed():
    global _DBND_RUN_PACKAGE_INSTALLED
    if _DBND_RUN_PACKAGE_INSTALLED is None:
        # cached answer
        try:
            import dbnd_run  # noqa: F401

            _DBND_RUN_PACKAGE_INSTALLED = True
        except Exception:
            _DBND_RUN_PACKAGE_INSTALLED = False

    return _DBND_RUN_PACKAGE_INSTALLED


def is_dbnd_orchestration_enabled():
    if get_dbnd_project_config().is_no_dbnd_orchestration:
        return False

    return is_dbnd_run_package_installed()


def is_dbnd_orchestration_via_airflow_enabled():
    return is_airflow_package_installed()


def is_airflow_package_installed():
    global _AIRFLOW_PACKAGE_INSTALLED
    if _AIRFLOW_PACKAGE_INSTALLED is None:
        # cached answer
        try:
            import airflow  # noqa: F401

            _AIRFLOW_PACKAGE_INSTALLED = True
        except Exception:
            _AIRFLOW_PACKAGE_INSTALLED = False

    return _AIRFLOW_PACKAGE_INSTALLED


def disable_airflow_package():
    global _AIRFLOW_PACKAGE_INSTALLED
    _AIRFLOW_PACKAGE_INSTALLED = False


def assert_airflow_package_installed():
    if not is_airflow_package_installed():
        raise friendly_error.config.missing_module("airflow")


def use_airflow_connections():
    from dbnd._core.configuration.dbnd_config import config

    return is_airflow_package_installed() and config.getboolean(
        "airflow", "use_connections"
    )
