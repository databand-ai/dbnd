import sys

from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled


######
## SUPPORT DBND TASK AS AIRFLOW OPERATORS
def is_in_airflow_dag_build_context():
    if "airflow" not in sys.modules:
        return False

    if not is_airflow_enabled():
        return False

    from dbnd_airflow.functional.dbnd_functional_dag import (
        is_in_airflow_dag_build_context as airflow__is_in_airflow_dag_build_context,
    )

    return airflow__is_in_airflow_dag_build_context()


def build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs):
    """
    wraps airflow import, so we don't import airflow from TaskFactory
    """
    from dbnd_airflow.functional.dbnd_functional_dag import (
        build_task_at_airflow_dag_context,
    )

    return build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs)
