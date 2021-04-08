from typing import List, Union

from airflow.models import DagModel, DagRun, Log, TaskInstance
from airflow.settings import Session
from sqlalchemy import and_, func, or_, tuple_

from dbnd_airflow_export.models import AirflowTaskInstance
from dbnd_airflow_export.plugin_old.metrics import measure_time, save_result_size
from dbnd_airflow_export.plugin_old.model import EDagRun


@save_result_size("find_new_dag_runs")
@measure_time
def find_new_dag_runs(
    last_seen_dagrun_id, extra_dag_runs_ids, extra_dag_runs_tuple, dag_ids, session
):
    new_runs_query = session.query(
        DagRun.id,
        DagRun.dag_id,
        DagRun.execution_date,
        DagRun.state,
        DagModel.is_paused,
    ).join(DagModel, DagModel.dag_id == DagRun.dag_id)

    if dag_ids:
        new_runs_query = new_runs_query.filter(DagRun.dag_id.in_(dag_ids))

    new_runs_filter_condition = or_(
        DagRun.id.in_(extra_dag_runs_ids),
        and_(DagRun.state == "running", DagModel.is_paused.is_(False)),
        tuple_(DagRun.dag_id, DagRun.execution_date).in_(extra_dag_runs_tuple),
    )

    if last_seen_dagrun_id is not None:
        new_runs_filter_condition = or_(
            new_runs_filter_condition, DagRun.id > last_seen_dagrun_id
        )

    new_runs_query = new_runs_query.filter(new_runs_filter_condition)

    new_runs = new_runs_query.all()

    return new_runs


@save_result_size("find_all_logs_grouped_by_runs")
@measure_time
def find_all_logs_grouped_by_runs(last_seen_log_id, dag_ids, session):
    # type: (int, List[str], Session) -> List[Log]

    if last_seen_log_id is None:
        return []

    if session.bind.dialect.name == "postgresql":
        events_field = func.array_agg(Log.event.distinct()).label("events")
    else:  # mysql, sqlite
        events_field = func.group_concat(Log.event.distinct()).label("events")

    if dag_ids:
        dag_ids_filter_condition = Log.dag_id.in_(dag_ids)
    else:
        dag_ids_filter_condition = Log.dag_id.isnot(None)

    logs_query = (
        session.query(
            func.max(Log.id).label("id"), Log.dag_id, Log.execution_date, events_field
        )
        .filter(and_(Log.id > last_seen_log_id, dag_ids_filter_condition))
        .group_by(Log.dag_id, Log.execution_date)
    )

    logs = logs_query.all()

    return logs


@measure_time
def find_max_dag_run_id(session):
    # type: (Session) -> Union[int, None]
    last_dag_run_id = session.query(func.max(DagRun.id)).scalar()
    return last_dag_run_id


@measure_time
def find_max_log_run_id(session):
    # type: (Session) -> Union[int, None]
    last_log_id = session.query(func.max(Log.id)).scalar()
    return last_log_id


@save_result_size("find_full_dag_runs")
@measure_time
def find_full_dag_runs(dag_run_ids, session):
    # type: (List[int], Session) -> (List[AirflowTaskInstance], List[EDagRun])

    result_fields = (
        session.query(*EDagRun.query_fields(), *AirflowTaskInstance.query_fields())
        .outerjoin(
            TaskInstance,
            (
                (TaskInstance.dag_id == DagRun.dag_id)
                & (TaskInstance.execution_date == DagRun.execution_date)
                | (TaskInstance.task_id.is_(None))
            ),
        )
        .filter(DagRun.id.in_(dag_run_ids))
        .all()
    )

    task_instances = []
    dag_runs = set()
    for fields in result_fields:
        dag_run_fields = fields[: len(EDagRun.db_fields)]
        dag_run = EDagRun.from_db_fields(*dag_run_fields)
        dag_runs.add(dag_run)

        task_instance_fields = fields[len(EDagRun.db_fields) :]
        if any(task_instance_fields):
            task_instance = AirflowTaskInstance(*task_instance_fields)
            task_instances.append(task_instance)

    return task_instances, dag_runs
