def operator_to_to_dbnd_task_id(operator):
    return getattr(operator, "dbnd_task_id", None)


def operator_to_dbnd_task(operator):
    from dbnd._core.current import get_task_by_task_id

    dbnd_task_id = operator_to_to_dbnd_task_id(operator)
    if dbnd_task_id:
        return get_task_by_task_id(dbnd_task_id)
    return None


def try_operator_to_dbnd_task(operator):
    from airflow.models import BaseOperator
    from dbnd._core.current import get_task_by_task_id

    if isinstance(
        operator, BaseOperator
    ):  # we are trying to support native Airflow Operators
        dbnd_task_id = operator_to_to_dbnd_task_id(operator)
        if dbnd_task_id:
            return get_task_by_task_id(dbnd_task_id)
    return None
