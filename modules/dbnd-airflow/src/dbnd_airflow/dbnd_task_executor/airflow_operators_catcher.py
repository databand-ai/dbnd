from airflow import DAG


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
