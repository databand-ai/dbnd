from airflow import DAG

from dbnd_airflow.airflow_utils import safe_get_context_manager_dag
from dbnd_airflow.config import get_dbnd_default_args


class DatabandOpCatcherDag(DAG):
    """
    we will use this Dag, when we have AirflowOperators in DatabandTasks
    for every operator `add_task` will be called, that will create DatabandTask
    so it can be registered and connected to graph
    """

    def add_task(self, task):
        super(DatabandOpCatcherDag, self).add_task(task)
        # task is AirflowOperator

        from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
            AirflowOperatorAsDbndTask,
        )

        AirflowOperatorAsDbndTask.build_airflow_task(airflow_operator=task, dag=self)


_dag_catcher = None


def get_databand_op_catcher_dag():
    import airflow  # noqa: F401

    if safe_get_context_manager_dag():
        # we are inside native airflow DAG or already have DatabandOpCatcherDag
        return None

    global _dag_catcher
    if not _dag_catcher:
        _dag_catcher = DatabandOpCatcherDag(
            dag_id="_dbnd_airflow_operator_catcher",
            default_args=get_dbnd_default_args(),
        )
    _dag_catcher.task_dict = {}
    return _dag_catcher
