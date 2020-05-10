import os


_airflow_bootstrap_applied = False


def dbnd_airflow_bootstrap():
    global _airflow_bootstrap_applied
    if _airflow_bootstrap_applied:
        return

    from dbnd._core.configuration.dbnd_config import config as dbnd_config
    from dbnd_airflow.airflow_override import patch_airflow_modules

    if dbnd_config.getboolean("airflow", "enable_dbnd_patches"):
        patch_airflow_modules()

    if os.name == "nt" and dbnd_config.getboolean("airflow", "enable_windows_support"):
        from dbnd_airflow.airflow_override.dbnd_airflow_windows import (
            patch_airflow_windows_support,
        )

        patch_airflow_windows_support()

    from dbnd_airflow.airflow_extensions.airflow_config import (
        init_airflow_sqlconn_by_dbnd,
    )

    init_airflow_sqlconn_by_dbnd()

    from dbnd_airflow.web.single_job_run_support import register_legacy_single_job_run

    register_legacy_single_job_run()

    _airflow_bootstrap_applied = True
