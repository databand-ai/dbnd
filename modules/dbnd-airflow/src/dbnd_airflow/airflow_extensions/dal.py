import logging

from airflow.models import TaskInstance
from airflow.utils.db import provide_session


logger = logging.getLogger(__name__)


@provide_session
def get_airflow_task_instance_state(task_run, session=None):
    TI = TaskInstance
    return (
        session.query(TI.state)
        .filter(
            TI.dag_id == task_run.task.ctrl.airflow_op.dag.dag_id,
            TI.task_id == task_run.task_af_id,
            TI.execution_date == task_run.run.execution_date,
        )
        .scalar()
    )


@provide_session
def get_airflow_task_instance(task_run, session=None):
    # type: (...)->TaskInstance
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == task_run.task.ctrl.airflow_op.dag.dag_id,
            TaskInstance.task_id == task_run.task_af_id,
            TaskInstance.execution_date == task_run.run.execution_date,
        )
        .first()
    )
    return ti


@provide_session
def update_airflow_task_instance_in_db(ti, session=None):
    session.merge(ti)
    session.commit()
