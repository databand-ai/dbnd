import os

from dbnd_airflow.tracking.airflow_patching import patch_airflow_context_vars


_airflow_bootstrap_applied = False


def dbnd_airflow_bootstrap():
    """
    all relevant patches for airflow execution
    """
    global _airflow_bootstrap_applied
    if _airflow_bootstrap_applied:
        return
    _airflow_bootstrap_applied = True  # prevent recursive call

    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    if dbnd_config.getboolean("airflow", "enable_dbnd_context_vars"):
        patch_airflow_context_vars()

    if os.name == "nt" and dbnd_config.getboolean("airflow", "enable_windows_support"):
        from dbnd_airflow.airflow_override.dbnd_airflow_windows import (
            patch_airflow_windows_support,
        )

        patch_airflow_windows_support()
