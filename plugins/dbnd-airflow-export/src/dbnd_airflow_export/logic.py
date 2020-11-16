import itertools
import logging
import sys

from functools import wraps
from timeit import default_timer

import flask
import pendulum
import six

from airflow.models import DagModel, DagRun, XCom
from airflow.utils.db import provide_session
from sqlalchemy import and_

from dbnd._core.run.databand_run import AD_HOC_DAG_PREFIX
from dbnd_airflow_export.helpers import _get_git_status, _get_log
from dbnd_airflow_export.model import EDag, EDagRun, ETaskInstance, ExportData


try:
    # in dbnd it might be overridden
    from airflow.models import original_TaskInstance as TaskInstance
except Exception:
    from airflow.models import TaskInstance


MAX_XCOM_SIZE_IN_BYTES = 10000
MAX_XCOM_LENGTH = 10

current_dags = {}


def measure_time(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        start = default_timer()
        result = f(*args, **kwargs)
        end = default_timer()
        if flask._app_ctx_stack.top is not None:
            if "perf_metrics" not in flask.g:
                flask.g.perf_metrics = {}
            flask.g.perf_metrics[f.__name__] = end - start
        return result

    return wrapped


def save_result_size(*names):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            result = f(*args, **kwargs)
            if flask._app_ctx_stack.top is not None:
                values = result
                if len(names) == 1:
                    values = (result,)
                for name, value_list in zip(names, values):
                    if "size_metrics" not in flask.g:
                        flask.g.size_metrics = {}
                    flask.g.size_metrics[name] = len(value_list)
            return result

        return wrapped

    return decorator


@save_result_size("current_dags")
@measure_time
def _load_dags_models(session):
    dag_models = session.query(DagModel).all()

    for dag_model in dag_models:
        # Exclude dbnd-run tagged runs
        if not dag_model.dag_id.startswith(AD_HOC_DAG_PREFIX):
            current_dags[dag_model.dag_id] = dag_model
    return current_dags


@measure_time
def get_airflow_regular_data(
    session,
    dagbag,
    since,
    include_logs=False,
    include_task_args=False,
    include_xcom=False,
    dag_ids=None,
    quantity=None,
):
    """
    Get first task instances that ended after since.
    Then get related DAG runs in the same time frame, DAG runs with no end date, or DAG runs with no tasks
    All DAGs are always exported since their amount is low.
    Amount of exported data is limited by quantity parameter which limits the number of task instances and DAG runs.
    """
    since = since or pendulum.datetime.min
    _load_dags_models(session)
    logging.info(
        "Collected %d dags. Trying to query task instances and dagruns from %s",
        len(current_dags),
        since,
    )

    task_instances, dag_runs = _get_task_instances(
        since, dag_ids, quantity, dagbag, include_logs, include_xcom, session
    )
    logging.info("%d task instances were found." % len(task_instances))

    task_end_dates = [
        task.end_date for task in task_instances if task.end_date is not None
    ]
    if not task_end_dates or not quantity or len(task_instances) < quantity:
        dag_run_end_date = pendulum.datetime.max
    else:
        dag_run_end_date = max(task_end_dates)

    dag_runs |= _get_dag_runs_without_tasks(
        since, dag_run_end_date, dag_ids, quantity, session
    )
    logging.info("%d dag runs were found." % len(dag_runs))

    if not task_instances and not dag_runs:
        return ExportData(since=since)

    dags_list = _get_dags(dagbag, include_task_args, dag_ids)

    logging.info(
        "Returning {} task instances, {} dag runs, {} dags".format(
            len(task_instances), len(dag_runs), len(dags_list)
        )
    )

    ed = ExportData(
        task_instances=task_instances, dag_runs=dag_runs, dags=dags_list, since=since,
    )

    return ed


@save_result_size("dags")
@measure_time
def _get_dags(dagbag, include_task_args, dag_ids):
    dag_models = [d for d in current_dags.values() if d]
    if dag_ids:
        dag_models = [dag for dag in dag_models if dag.dag_id in dag_ids]

    number_of_dags_not_in_dag_bag = 0
    dags_list = []
    git_commit, is_committed = _get_git_status(dagbag.dag_folder)

    for dag_model in dag_models:
        dag_from_dag_bag = dagbag.get_dag(dag_model.dag_id)
        if dagbag.get_dag(dag_model.dag_id):
            dag = EDag.from_dag(
                dag_from_dag_bag,
                dag_model,
                dagbag.dag_folder,
                include_task_args,
                git_commit,
                is_committed,
            )
        else:
            dag = EDag.from_dag(
                dag_model,
                dag_model,
                dagbag.dag_folder,
                include_task_args,
                git_commit,
                is_committed,
            )
            number_of_dags_not_in_dag_bag += 1
        dags_list.append(dag)

    if number_of_dags_not_in_dag_bag > 0:
        logging.info(
            "Found {} dags not in dagbag".format(number_of_dags_not_in_dag_bag)
        )
    return dags_list


@measure_time
def get_airflow_incomplete_data(
    session,
    dagbag,
    since,
    dag_ids,
    include_task_args,
    incomplete_offset=0,
    quantity=100,
):
    since = since or pendulum.datetime.min
    _load_dags_models(session)
    logging.info(
        "Collected %d dags. Trying to query incomplete task instances and dagruns from %s",
        len(current_dags),
        since,
    )

    task_instances, dag_runs = _get_task_instances_without_date(
        since=since,
        dag_ids=dag_ids,
        dagbag=dagbag,
        session=session,
        incomplete_offset=incomplete_offset,
        page_size=quantity,
    )
    logging.info(
        "Found {} task instances with no end_date from {} dag runs".format(
            len(task_instances), len(dag_runs)
        )
    )

    dag_runs_without_date = _get_dag_runs_without_date(
        since=since,
        dag_ids=dag_ids,
        session=session,
        incomplete_offset=incomplete_offset,
        page_size=quantity,
    )
    logging.info("Found {} dag runs with no end_date".format(len(dag_runs)))

    dag_runs |= dag_runs_without_date

    dags_list = _get_dags(dagbag, include_task_args, dag_ids)

    logging.info(
        "Returning {} task instances, {} dag runs, {} dags".format(
            len(task_instances), len(dag_runs), len(dags_list)
        )
    )

    ed = ExportData(
        task_instances=task_instances, dag_runs=dag_runs, dags=dags_list, since=since,
    )

    return ed


@save_result_size("dag_runs_without_date")
@measure_time
def _get_dag_runs_without_date(
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


@save_result_size("dag_runs_without_tasks")
@measure_time
def _get_dag_runs_without_tasks(start_date, end_date, dag_ids, quantity, session):
    # Bring all dag runs with no tasks (limit the number)
    dagruns_query = session.query(*EDagRun.query_fields()).filter(
        and_(DagRun.end_date > start_date, DagRun.end_date <= end_date)
    )

    if dag_ids:
        dagruns_query = dagruns_query.filter(DagRun.dag_id.in_(dag_ids))

    # We reached a point where there are no more task, but can have potentially large number of dag runs with no tasks
    # so let's limit them. In the next fetch we'll get the next runs.
    if quantity is not None and end_date == pendulum.datetime.max:
        dagruns_query = dagruns_query.order_by(DagRun.end_date).limit(quantity)

    return set(EDagRun.from_db_fields(*fields) for fields in dagruns_query.all())


@save_result_size("task_instances_without_date")
@measure_time
def _get_task_instances_without_date(
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
            and_(TaskInstance.end_date.is_(None), TaskInstance.execution_date > since)
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
    task_instances = []
    dag_runs = set()
    for fields in results:
        ti_fields = fields[: len(ETaskInstance.db_fields)]
        dr_fields = fields[len(ETaskInstance.db_fields) :]
        task_instances.append(
            _build_task_instance(ti_fields, dagbag, False, False, session)
        )
        dag_runs.add(EDagRun.from_db_fields(*dr_fields))

    return task_instances, dag_runs


@save_result_size("task_instances", "dag_runs")
@measure_time
def _get_task_instances(
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
    task_instances = []
    dag_runs = set()
    for fields in results:
        ti_fields = fields[: len(ETaskInstance.db_fields)]
        dr_fields = fields[len(ETaskInstance.db_fields) :]
        task_instances.append(
            _build_task_instance(ti_fields, dagbag, include_xcom, include_logs, session)
        )
        dag_runs.add(EDagRun.from_db_fields(*dr_fields))

    return task_instances, dag_runs


def _build_task_instance(ti_fields, dagbag, include_xcom, include_logs, session):
    eti = ETaskInstance(*ti_fields)
    if include_xcom or include_logs:
        dag_from_dag_bag = dagbag.get_dag(eti.dag_id)
        if dag_from_dag_bag:
            if include_xcom:
                xcom_dict = _get_task_instance_xcom_dict(
                    dag_id=eti.dag_id,
                    task_id=eti.task_id,
                    execution_date=eti.execution_date,
                )
                eti.xcom_dict = xcom_dict
            if include_logs:
                task = (
                    dag_from_dag_bag.get_task(eti.task_id)
                    if dag_from_dag_bag and dag_from_dag_bag.has_task(eti.task_id)
                    else None
                )
                if task:
                    ti = (
                        session.query(TaskInstance)
                        .filter(
                            TaskInstance.task_id == eti.task_id,
                            TaskInstance.dag_id == eti.dag_id,
                            TaskInstance.execution_date == eti.execution_date,
                        )
                        .first()
                    )
                    eti.log_body = _get_log(ti, task)
    return eti


@measure_time
def _get_task_instance_xcom_dict(dag_id, task_id, execution_date):
    try:
        results = XCom.get_many(
            dag_ids=dag_id, task_ids=task_id, execution_date=execution_date
        )
        if not results:
            return {}

        xcom_dict = {xcom.key: str(xcom.value) for xcom in results}

        sliced_xcom = (
            dict(itertools.islice(xcom_dict.items(), MAX_XCOM_LENGTH))
            if len(xcom_dict) > MAX_XCOM_LENGTH
            else xcom_dict
        )
        for key, value in six.iteritems(sliced_xcom):
            sliced_xcom[key] = shorten_xcom_value(value)

        return sliced_xcom
    except Exception as e:
        logging.info("Failed to get xcom dict. Exception: {}".format(e))
        return {}


def shorten_xcom_value(xcom_value):
    if sys.getsizeof(xcom_value) <= MAX_XCOM_SIZE_IN_BYTES:
        return xcom_value

    diff = len(xcom_value) - MAX_XCOM_SIZE_IN_BYTES
    return xcom_value[-diff:]


@measure_time
@provide_session
def get_current_dag_model(dag_id, session=None):
    # MONKEY PATCH for old DagModel.get_current to try cache first
    if dag_id not in current_dags:
        current_dags[dag_id] = (
            session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        )

    return current_dags[dag_id]
