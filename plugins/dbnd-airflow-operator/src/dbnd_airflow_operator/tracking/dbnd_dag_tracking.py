from dbnd import dbnd_run_stop
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
    dbnd_run_start_airflow_dag_task,
)


def apply_dbnd_on_operator(operator):
    pre_execute = operator.pre_execute
    post_execute = operator.post_execute

    def new_pre_execute(*args, **kwargs):
        pre_execute(*args, **kwargs)

        ti = kwargs["context"]["task_instance"]
        dbnd_run_start_airflow_dag_task(
            dag_id=ti.dag_id, execution_date=ti.execution_date, task_id=ti.task_id
        )

    def new_post_execute(*args, **kwargs):
        dbnd_run_stop(at_exit=False)
        post_execute(*args, **kwargs)

    operator.pre_execute = new_pre_execute
    operator.post_execute = new_post_execute


def apply_dbnd_on_dag(dag):
    for task in dag.tasks:
        apply_dbnd_on_operator(task)
