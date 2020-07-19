import airflow.utils.operator_helpers

from dbnd._core.current import try_get_databand_run


def context_to_airflow_vars(context, in_env_var_format=False):
    # original_context_to_airflow_vars is created during function override in patch_models()
    params = airflow.utils.operator_helpers._original_context_to_airflow_vars(
        context=context, in_env_var_format=in_env_var_format
    )
    if in_env_var_format:
        dbnd_run = try_get_databand_run()
        if dbnd_run:
            params.update(dbnd_run.get_context_spawn_env())

    try_number = str(context['task_instance'].try_number)
    params.update({"AIRFLOW_CTX_TRY_NUMBER": try_number})
    return params
