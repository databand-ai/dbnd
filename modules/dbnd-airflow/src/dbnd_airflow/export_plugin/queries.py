# © Copyright Databand.ai, an IBM Company 2022


from airflow.models import DagModel, DagRun
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.orm import joinedload, relationship

from dbnd_airflow.export_plugin.metrics import measure_time, save_result_size
from dbnd_airflow.export_plugin.models import AirflowTaskInstance, DagRunState, EDagRun
from dbnd_airflow.export_plugin.utils import AIRFLOW_VERSION_BEFORE_2_2


if AIRFLOW_VERSION_BEFORE_2_2:

    class DagRunExt(DagRun):
        task_instances = relationship(
            "TaskInstance",
            primaryjoin="and_(TaskInstance.dag_id == foreign(DagRunExt.dag_id), TaskInstance.execution_date == foreign(DagRunExt.execution_date))",
            uselist=True,
        )

    DagRunModel = DagRunExt
else:
    DagRunModel = DagRun


def _build_query_for_subdag_prefixes(column, dag_ids, excluded_dag_ids):
    subdag_dag_id_prefixes = (f"{dag_id}." for dag_id in dag_ids or excluded_dag_ids)
    if dag_ids:
        return or_(
            column.in_(dag_ids),
            *(column.startswith(prefix) for prefix in subdag_dag_id_prefixes),
        )
    return and_(
        not_(column.in_(excluded_dag_ids)),
        *(not_(column.startswith(prefix)) for prefix in subdag_dag_id_prefixes),
    )


def _get_new_dag_runs_base_query(dag_ids, excluded_dag_ids, include_subdags, session):
    new_runs_base_query = session.query(
        DagRun.id,
        DagRun.dag_id,
        DagRun.execution_date,
        DagRun.state,
        DagModel.is_paused,
    ).join(DagModel, DagModel.dag_id == DagRun.dag_id)

    if dag_ids or excluded_dag_ids:
        new_runs_base_query = new_runs_base_query.filter(
            _build_query_for_subdag_prefixes(DagRun.dag_id, dag_ids, excluded_dag_ids)
        )

    if not include_subdags:
        new_runs_base_query = new_runs_base_query.filter(DagModel.is_subdag.is_(False))

    return new_runs_base_query


def _get_new_dag_runs_filter_condition(last_seen_dagrun_id, extra_dag_runs_ids):
    new_runs_filter_condition = or_(
        DagRun.id.in_(extra_dag_runs_ids),
        and_(DagRun.state == DagRunState.RUNNING, DagModel.is_paused.is_(False)),
    )

    if last_seen_dagrun_id is not None:
        new_runs_filter_condition = or_(
            new_runs_filter_condition, DagRun.id > last_seen_dagrun_id
        )

    return new_runs_filter_condition


@save_result_size("find_new_dag_runs")
@measure_time
def find_new_dag_runs(
    last_seen_dagrun_id,
    extra_dag_runs_ids,
    dag_ids,
    excluded_dag_ids,
    include_subdags,
    session,
):
    new_runs_base_query = _get_new_dag_runs_base_query(
        dag_ids, excluded_dag_ids, include_subdags, session
    )
    new_runs_filter_condition = _get_new_dag_runs_filter_condition(
        last_seen_dagrun_id, extra_dag_runs_ids
    )
    new_runs_query = new_runs_base_query.filter(new_runs_filter_condition)
    new_runs = new_runs_query.all()
    return set(new_runs)


@measure_time
def find_max_dag_run_id(session):
    # type: (Session) -> Union[int, None]
    last_dag_run_id = session.query(func.max(DagRun.id)).scalar()
    return last_dag_run_id


@save_result_size("find_full_dag_runs")
@measure_time
def find_full_dag_runs(dag_run_ids, session):
    # type: (List[int], Session) -> (List[AirflowTaskInstance], List[EDagRun])
    result_dag_runs = (
        session.query(DagRunModel)
        .options(joinedload(DagRunModel.task_instances))
        .filter(DagRunModel.id.in_(dag_run_ids))
        .all()
    )

    task_instances = []
    dag_runs = set()
    for dag_run in result_dag_runs:
        new_dag_run = EDagRun.from_db_fields(
            dag_id=dag_run.dag_id,
            dagrun_id=dag_run.id,
            start_date=dag_run.start_date,
            state=dag_run.state,
            end_date=dag_run.end_date,
            execution_date=dag_run.execution_date,
            conf=dag_run.conf,
            run_id=dag_run.run_id,
        )
        dag_runs.add(new_dag_run)

        for task_instance in dag_run.task_instances:
            new_task_instance = AirflowTaskInstance(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                execution_date=task_instance.execution_date,
                state=task_instance.state,
                try_number=task_instance._try_number,
                start_date=task_instance.start_date,
                end_date=task_instance.end_date,
            )
            task_instances.append(new_task_instance)

    return task_instances, dag_runs
