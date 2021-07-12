import getpass

from airflow import DAG, models, settings


def safe_get_context_manager_dag():
    """
    Try to find the CONTEXT_MANAGER_DAG object inside airflow.
    It was moved between versions, so we look for it in all the hiding places that we know of.
    """
    if hasattr(settings, "CONTEXT_MANAGER_DAG"):
        return settings.CONTEXT_MANAGER_DAG
    elif hasattr(DAG, "_CONTEXT_MANAGER_DAG"):
        return DAG._CONTEXT_MANAGER_DAG
    elif hasattr(models, "_CONTEXT_MANAGER_DAG"):
        return models._CONTEXT_MANAGER_DAG
    return None


def hostname_as_username():
    """
    This function is used to speed up airflow.
    When not redirected to this method, airflow retrieves the host name of the machine using the `socket` library.
    The calls to `socket` are extremely slow, so we redirect it to the native python implementation
    """
    return getpass.getuser()


def is_airflow_support_template_fields():
    from airflow import __version__

    return __version__ not in {"1.10.0", "1.10.1"}


def DbndFunctionalOperator_1_10_0(
    task_id, dbnd_task_type, dbnd_task_params_fields, **ctor_kwargs
):
    """
    Workaround for backwards compatibility with Airflow 1.10.0,
    dynamically creating new operator class (that inherits AirflowDagDbndOperator).
    This is used because XCom templates for each operator are found in a class attribute
    and we have different XCom templates for each INSTANCE of the operator.
    Also, the template_fields attribute has a different value for each task, so we must ensure that they
    don't get mixed up.
    """
    template_fields = dbnd_task_params_fields
    from dbnd_airflow.functional.dbnd_functional_operator import DbndFunctionalOperator

    new_op = type(
        dbnd_task_type, (DbndFunctionalOperator,), {"template_fields": template_fields}
    )
    op = new_op(
        task_id=task_id,
        dbnd_task_type=dbnd_task_type,
        dbnd_task_params_fields=dbnd_task_params_fields,
        **ctor_kwargs
    )
    return op
