# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.log.dbnd_log import dbnd_log_exception, dbnd_log_init_msg
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
        dbnd_log_init_msg("patched module.policy with dbnd auto tracking")
    if hasattr(module, "task_policy"):
        # airflow >= 2.0, https://airflow.apache.org/docs/apache-airflow/stable/concepts/cluster-policies.html
        new_policy = _wrap_policy_with_dbnd_track_task(module.task_policy)
        module.task_policy = new_policy
        dbnd_log_init_msg("patched module.task_policy with dbnd auto tracking")


def _add_tracking_to_policy():
    try:
        # User can have this file or not
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
    """Add tracking to all tasks as part of airflow policy"""
    try:
        _add_tracking_to_policy()
    except Exception as ex:
        dbnd_log_exception("Failed to add dbnd auto tracking to airflow policy %s" % ex)


def patch_airflow_context_vars():
    """Used for tracking bash operators"""

    from dbnd._core.utils.object_utils import patch_models
    from dbnd_airflow.airflow_override.operator_helpers import context_to_airflow_vars

    import airflow.models.taskinstance  # isort:skip

    modules_to_patch = [airflow.utils.operator_helpers, airflow.models.taskinstance]
    patches = []
    for module in modules_to_patch:
        if hasattr(module, "context_to_airflow_vars"):
            patches.append((module, "context_to_airflow_vars", context_to_airflow_vars))
    patch_models(patches)


def patch_snowflake_hook():
    # In order to use this patch the user need to have both `dbnd-snowflake` and `snowflake` installed
    try:
        import snowflake  # noqa: F401

        from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

        from dbnd_snowflake.sql_tracking import (
            config_base_target_reporter,
            patch_airflow_db_hook,
        )
    except ImportError:
        # one of them is not available
        return

    patch_airflow_db_hook(SnowflakeHook, config_base_target_reporter)
