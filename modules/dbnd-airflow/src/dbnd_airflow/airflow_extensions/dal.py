from airflow.models import TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State

from dbnd._core.utils.timezone import utcnow


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


@provide_session
def schedule_task_instance_for_retry(
    task_run, retry_count, retry_delay, increment_try_number, session=None
):
    task_instance = get_airflow_task_instance(task_run, session=session)
    task_instance.max_tries = retry_count

    if task_instance.try_number <= task_instance.max_tries:
        task_run.task.task_retries = retry_count
        task_run.task.task_retry_delay = retry_delay
        task_instance.state = State.UP_FOR_RETRY
        # Ensure that end date has a value. If it does not - airflow crashes when calculating next retry datetime
        task_instance.end_date = task_instance.end_date or utcnow()
        if increment_try_number:
            task_instance._try_number += 1
        update_airflow_task_instance_in_db(task_instance, session=session)
        return True
    else:
        return False
