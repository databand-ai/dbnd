import logging

from dbnd_airflow.tracking.dbnd_dag_tracking import track_task


def _wrap_policy(policy):
    def new_policy(task):
        policy(task)
        track_task(task)

    return new_policy


def _patch_policy(module):
    if not hasattr(module, "policy"):
        return
    new_policy = _wrap_policy(module.policy)
    module.policy = new_policy


def _add_tracking_to_policy():
    try:
        import airflow_local_settings

        _patch_policy(airflow_local_settings)
    except ImportError:
        pass

    from airflow import settings

    _patch_policy(settings)


def add_tracking_to_policy():
    """ Add tracking to all tasks as part of airflow policy """
    try:
        _add_tracking_to_policy()
    except Exception:
        logging.exception("Failed to add tracking in policy")


def patch_airflow_context_vars():
    """ used for tracking bash operators """
    import airflow
    from airflow.utils import operator_helpers
    from dbnd_airflow.airflow_override.operator_helpers import context_to_airflow_vars

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
