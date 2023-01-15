# © Copyright Databand.ai, an IBM Company 2022

import os


_airflow_bootstrap_applied = False


def dbnd_run_airflow_bootstrap():
    """
    all relevant patches for airflow execution
    """
    global _airflow_bootstrap_applied
    if _airflow_bootstrap_applied:
        return
    _airflow_bootstrap_applied = True  # prevent recursive call

    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    if os.name == "nt" and dbnd_config.getboolean("airflow", "enable_windows_support"):
        from dbnd_run.airflow.airflow_override.dbnd_airflow_windows import (
            patch_airflow_windows_support,
        )

        patch_airflow_windows_support()
