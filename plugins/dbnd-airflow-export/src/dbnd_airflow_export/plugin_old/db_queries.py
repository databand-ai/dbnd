from airflow.models import DagRun
from sqlalchemy import and_

from dbnd_airflow_export.datetime_utils import pendulum_max_dt
from dbnd_airflow_export.plugin_old.metrics import measure_time, save_result_size
from dbnd_airflow_export.plugin_old.model import EDagRun, ETaskInstance
from dbnd_airflow_export.plugin_old.task_instance_building import build_task_instance


try:
    # in dbnd it might be overridden
    from airflow.models import original_TaskInstance as TaskInstance
except Exception:
    from airflow.models import TaskInstance


@save_result_size("dag_runs_without_date")
@measure_time
def get_dag_runs_without_end_date(
    since, dag_ids, session, page_size=100, incomplete_offset=0
):
    dagruns_query = session.query(*EDagRun.query_fields()).filter(
        and_(DagRun.end_date.is_(None)), DagRun.execution_date > since
    )

    if dag_ids:
        dagruns_query = dagruns_query.filter(DagRun.dag_id.in_(dag_ids))

    dagruns_query = (
        dagruns_query.order_by(DagRun.id).limit(page_size).offset(incomplete_offset)
    )

    return set(EDagRun.from_db_fields(*fields) for fields in dagruns_query.all())


@save_result_size("get_dag_runs_within_time_window")
@measure_time
def get_dag_runs_within_time_window(start_date, end_date, dag_ids, quantity, session):
    # Bring all dag runs with no tasks (limit the number)
    dagruns_query = session.query(*EDagRun.query_fields()).filter(
        and_(DagRun.end_date > start_date, DagRun.end_date <= end_date)
    )

    if dag_ids:
        dagruns_query = dagruns_query.filter(DagRun.dag_id.in_(dag_ids))

    # We reached a point where there are no more task, but can have potentially large number of dag runs with no tasks
    # so let's limit them. In the next fetch we'll get the next runs.
    if quantity is not None and end_date == pendulum_max_dt:
        dagruns_query = dagruns_query.order_by(DagRun.end_date).limit(quantity)

    return set(EDagRun.from_db_fields(*fields) for fields in dagruns_query.all())


@save_result_size("task_instances_without_date")
@measure_time
def get_task_instances_without_end_date(
    since, dag_ids, dagbag, session, page_size=100, incomplete_offset=0
):
    task_instances_query = (
        session.query(*ETaskInstance.query_fields(), *EDagRun.query_fields())
        .join(
            DagRun,
            (TaskInstance.dag_id == DagRun.dag_id)
            & (TaskInstance.execution_date == DagRun.execution_date),
        )
        .filter(
            and_(
                DagRun.end_date.is_(None),
                TaskInstance.end_date.is_(None),
                TaskInstance.execution_date > since,
            )
        )
    )

    if dag_ids:
        task_instances_query = task_instances_query.filter(
            TaskInstance.dag_id.in_(dag_ids)
        )

    task_instances_query = (
        task_instances_query.order_by(
            TaskInstance.task_id, TaskInstance.dag_id, TaskInstance.execution_date
        )
        .limit(page_size)
        .offset(incomplete_offset)
    )

    results = task_instances_query.all()

    task_instances, dag_runs = _create_task_instances_and_dag_runs(
        results, dagbag, False, False, session
    )

    return task_instances, dag_runs


@save_result_size("completed_task_instances", "completed_dag_runs")
@measure_time
def get_completed_task_instances_and_dag_runs(
    since, dag_ids, quantity, dagbag, include_logs, include_xcom, session
):
    task_instances_query = (
        session.query(*ETaskInstance.query_fields(), *EDagRun.query_fields())
        .join(
            DagRun,
            (TaskInstance.dag_id == DagRun.dag_id)
            & (TaskInstance.execution_date == DagRun.execution_date),
        )
        .filter(TaskInstance.end_date > since)
    )

    if dag_ids:
        task_instances_query = task_instances_query.filter(
            TaskInstance.dag_id.in_(dag_ids)
        )

    if quantity is not None:
        task_instances_query = task_instances_query.order_by(
            TaskInstance.end_date
        ).limit(quantity)

    results = task_instances_query.all()

    task_instances, dag_runs = _create_task_instances_and_dag_runs(
        results, dagbag, include_xcom, include_logs, session
    )

    return task_instances, dag_runs


@save_result_size("incomplete_task_instances", "incomplete_dag_runs")
@measure_time
def get_incomplete_task_instances_from_completed_dag_runs(
    since, dag_ids, dagbag, max_quantity, offset, session
):
    task_instances_query = (
        session.query(*ETaskInstance.query_fields(), *EDagRun.query_fields())
        .join(
            DagRun,
            (TaskInstance.dag_id == DagRun.dag_id)
            & (TaskInstance.execution_date == DagRun.execution_date),
        )
        .filter(and_(TaskInstance.end_date.is_(None), DagRun.end_date > since))
    )

    if dag_ids:
        task_instances_query = task_instances_query.filter(DagRun.dag_id.in_(dag_ids))

    task_instances_query = (
        task_instances_query.order_by(DagRun.end_date, TaskInstance.task_id)
        .limit(max_quantity)
        .offset(offset)
    )
    results = task_instances_query.all()

    task_instances, dag_runs = _create_task_instances_and_dag_runs(
        results, dagbag, False, False, session
    )

    return task_instances, dag_runs


def _create_task_instances_and_dag_runs(
    results, dagbag, include_xcom, include_logs, session
):
    task_instances = []
    dag_runs = set()
    for fields in results:
        ti_fields = fields[: len(ETaskInstance.db_fields)]
        dr_fields = fields[len(ETaskInstance.db_fields) :]
        task_instances.append(
            build_task_instance(ti_fields, dagbag, include_xcom, include_logs, session)
        )
        dag_runs.add(EDagRun.from_db_fields(*dr_fields))

    return task_instances, dag_runs
