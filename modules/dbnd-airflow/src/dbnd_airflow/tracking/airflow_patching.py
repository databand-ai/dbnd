import logging

from dbnd_airflow.tracking.dbnd_dag_tracking import track_task


def _wrap_policy_with_dbnd_track_task(policy):
    if policy and getattr(policy, "_dbnd_patched", None):
        # already wrapped - don't double patch, return as is
        return policy

    def dbnd_track_task_policy(task):
        # Call original policy
        policy(task)
        # Wrap it with dbnd tracking
        track_task(task)

    dbnd_track_task_policy._dbnd_patched = True
    return dbnd_track_task_policy


def _patch_policy(module):
    if hasattr(module, "policy"):
        # airflow < 2.0, https://airflow.apache.org/docs/apache-airflow/1.10.10/concepts.html#cluster-policy
        new_policy = _wrap_policy_with_dbnd_track_task(module.policy)
        module.policy = new_policy
    if hasattr(module, "task_policy"):
        # airflow >= 2.0, https://airflow.apache.org/docs/apache-airflow/stable/concepts/cluster-policies.html
        new_policy = _wrap_policy_with_dbnd_track_task(module.task_policy)
        module.task_policy = new_policy


def _add_tracking_to_policy():
    try:
        # Use can have this file or not
        import airflow_local_settings

        _patch_policy(airflow_local_settings)
        # we want to proceed and patch dagbag.settings as well
        # 1. _patch_policy is idempotent
        # 2. this code runs after local_settings are read
        # 3. there could be situation that local_settings exist but without policy
        #    (so global settings.policy is used anyway)
    except ImportError:
        pass

    from airflow.models.dagbag import settings

    _patch_policy(settings)


def add_tracking_to_policy():
    """ Add tracking to all tasks as part of airflow policy """
    try:
        _add_tracking_to_policy()
    except Exception as e:
        logging.exception("Failed to add tracking in policy")


def patch_airflow_context_vars():
    """ Used for tracking bash operators """
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


def patch_snowflake_hook():
    # In order to use this patch the user need to have both `dbnd-snowflake` and `snowflake` installed
    try:
        from dbnd_snowflake.sql_tracking import (
            patch_airflow_db_hook,
            config_base_target_reporter,
        )
        import snowflake
        from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
    except ImportError:
        # one of them is not available
        return

    patch_airflow_db_hook(SnowflakeHook, config_base_target_reporter)
