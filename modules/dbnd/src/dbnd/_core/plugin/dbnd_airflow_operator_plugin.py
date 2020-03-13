_AIRFLOW_OPERATOR_ENABLED = None


def _is_airflow_operator_enabled():
    try:
        import dbnd_airflow_operator

        return True
    except ImportError:
        return False


def is_airflow_operator_enabled():
    global _AIRFLOW_OPERATOR_ENABLED
    if _AIRFLOW_OPERATOR_ENABLED is None:
        _AIRFLOW_OPERATOR_ENABLED = _is_airflow_operator_enabled()
    return _AIRFLOW_OPERATOR_ENABLED


######
## SUPPORT DBND TASK AS AIRFLOW OPERATORS
def is_in_airflow_dag_build_context():
    if not is_airflow_operator_enabled():
        return False

    from dbnd_airflow_operator.dbnd_functional_dag import (
        is_in_airflow_dag_build_context as airflow__is_in_airflow_dag_build_context,
    )

    return airflow__is_in_airflow_dag_build_context()


def build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs):
    from dbnd_airflow_operator.dbnd_functional_dag import (
        build_task_at_airflow_dag_context,
    )

    return build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs)
