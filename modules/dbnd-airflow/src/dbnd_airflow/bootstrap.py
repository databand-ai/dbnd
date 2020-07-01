import os


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
        import airflow
        from airflow.utils import operator_helpers
        from dbnd_airflow.airflow_override.operator_helpers import (
            context_to_airflow_vars,
        )

        from dbnd._core.utils.object_utils import patch_models

        if hasattr(airflow.utils.operator_helpers, "context_to_airflow_vars"):
            patches = [
                (
                    airflow.utils.operator_helpers,
                    "context_to_airflow_vars",
                    context_to_airflow_vars,
                )
            ]
            patch_models(patches)

    if os.name == "nt" and dbnd_config.getboolean("airflow", "enable_windows_support"):
        from dbnd_airflow.airflow_override.dbnd_airflow_windows import (
            patch_airflow_windows_support,
        )

        patch_airflow_windows_support()

    from dbnd_airflow.web.single_job_run_support import register_legacy_single_job_run

    register_legacy_single_job_run()
