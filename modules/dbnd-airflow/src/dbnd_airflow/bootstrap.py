# © Copyright Databand.ai, an IBM Company 2022


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

    patch_airflow_context_vars()
