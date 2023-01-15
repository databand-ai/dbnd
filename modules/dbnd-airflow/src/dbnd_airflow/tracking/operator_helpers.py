# © Copyright Databand.ai, an IBM Company 2022

import airflow.utils.operator_helpers

from dbnd._core.current import try_get_current_task_run
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    extend_airflow_ctx_with_dbnd_tracking_info,
)
from dbnd_airflow.utils import get_airflow_instance_uid


def context_to_airflow_vars(context, in_env_var_format=False):
    # original_context_to_airflow_vars is created during function override in patch_models()
    params = airflow.utils.operator_helpers._original_context_to_airflow_vars(
        context=context, in_env_var_format=in_env_var_format
    )
    if in_env_var_format:
        task_run = try_get_current_task_run()  # type: TaskRun
        if task_run:
            params = extend_airflow_ctx_with_dbnd_tracking_info(task_run, params)

    try_number = str(context["task_instance"].try_number)
    params.update(
        {
            "AIRFLOW_CTX_TRY_NUMBER": try_number,
            "AIRFLOW_CTX_UID": get_airflow_instance_uid(),
        }
    )
    return params
