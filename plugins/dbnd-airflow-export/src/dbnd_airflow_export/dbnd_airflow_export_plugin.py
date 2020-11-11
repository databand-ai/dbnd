import datetime
import itertools
import json
import logging
import os
import sys
import traceback

from functools import wraps
from timeit import default_timer

import flask
import flask_admin
import flask_appbuilder
import pendulum
import pkg_resources
import six

from airflow.configuration import conf
from airflow.models import BaseOperator, DagModel, DagRun, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.utils.net import get_hostname
from airflow.version import version as airflow_version
from flask import Response
from sqlalchemy import and_

from dbnd._core.run.databand_run import AD_HOC_DAG_PREFIX


DEFAULT_DAYS_PERIOD = 30
TASK_ARG_TYPES = (str, float, bool, int, datetime.datetime)

current_dags = {}

MAX_LOGS_SIZE_IN_BYTES = 10000
MAX_XCOM_SIZE_IN_BYTES = 10000
MAX_XCOM_LENGTH = 10

try:
    # in dbnd it might be overridden
    from airflow.models import original_TaskInstance as TaskInstance
except Exception:
    from airflow.models import TaskInstance


### Exceptions ###


class EmptyAirflowDatabase(Exception):
    pass


### Plugin Business Logic ###


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
def _get_airflow_data(
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
def _get_airflow_incomplete_data(
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
def _get_current_dag_model(dag_id, session=None):
    # MONKEY PATCH for old DagModel.get_current to try cache first
    if dag_id not in current_dags:
        current_dags[dag_id] = (
            session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        )

    return current_dags[dag_id]


class ETask(object):
    def __init__(
        self,
        upstream_task_ids=None,
        downstream_task_ids=None,
        task_type=None,
        task_source_code=None,
        task_module_code=None,
        dag_id=None,
        task_id=None,
        retries=None,
        command=None,
        task_args=None,
    ):
        self.upstream_task_ids = list(upstream_task_ids)  # type: List[str]
        self.downstream_task_ids = list(downstream_task_ids)  # type: List[str]
        self.task_type = task_type
        self.task_source_code = task_source_code
        self.task_module_code = task_module_code
        self.dag_id = dag_id
        self.task_id = task_id
        self.retries = retries
        self.command = command
        self.task_args = task_args

    @staticmethod
    def from_task(t, include_task_args):
        # type: (BaseOperator, bool) -> ETask
        return ETask(
            upstream_task_ids=t.upstream_task_ids,
            downstream_task_ids=t.downstream_task_ids,
            task_type=t.task_type,
            task_source_code=_get_source_code(t),
            task_module_code=_get_module_code(t),
            dag_id=t.dag_id,
            task_id=t.task_id,
            retries=t.retries,
            command=_get_command_from_operator(t),
            task_args=_extract_args_from_dict(vars(t)) if include_task_args else {},
        )

    def as_dict(self):
        return dict(
            upstream_task_ids=self.upstream_task_ids,
            downstream_task_ids=self.downstream_task_ids,
            task_type=self.task_type,
            task_source_code=self.task_source_code,
            task_module_code=self.task_module_code,
            dag_id=self.dag_id,
            task_id=self.task_id,
            retries=self.retries,
            command=self.command,
            task_args=self.task_args,
        )


class ETaskInstance(object):
    db_fields = [
        "execution_date",
        "dag_id",
        "state",
        "_try_number",
        "task_id",
        "start_date",
        "end_date",
    ]

    @classmethod
    def query_fields(cls):
        return [getattr(TaskInstance, key) for key in cls.db_fields]

    def __init__(
        self,
        execution_date,
        dag_id,
        state,
        try_number,
        task_id,
        start_date,
        end_date,
        log_body=None,
        xcom_dict=None,
    ):
        self.execution_date = execution_date
        self.dag_id = dag_id
        self.state = state
        self.try_number = try_number
        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date
        self.log_body = log_body
        self.xcom_dict = xcom_dict or {}

    def as_dict(self):
        return dict(
            execution_date=self.execution_date,
            dag_id=self.dag_id,
            state=self.state,
            try_number=self.try_number,
            task_id=self.task_id,
            start_date=self.start_date,
            end_date=self.end_date,
            log_body=self.log_body,
            xcom_dict=self.xcom_dict,
        )


### Models ###


class EDagRun(object):
    db_fields = [
        "dag_id",
        "id",
        "start_date",
        "state",
        "end_date",
        "execution_date",
        "conf",
    ]

    @classmethod
    def query_fields(cls):
        return [getattr(DagRun, key) for key in cls.db_fields]

    def __init__(
        self, dag_id, dagrun_id, start_date, state, end_date, execution_date, task_args
    ):
        self.dag_id = dag_id
        self.dagrun_id = dagrun_id
        self.start_date = start_date
        self.state = state
        self.end_date = end_date
        self.execution_date = execution_date
        self.task_args = task_args

    @classmethod
    def from_db_fields(
        cls, dag_id, dagrun_id, start_date, state, end_date, execution_date, conf
    ):
        return cls(
            dag_id,
            dagrun_id,
            start_date,
            state,
            end_date,
            execution_date,
            (_extract_args_from_dict(conf) if conf else {}),
        )

    def __hash__(self):
        return hash(self.dagrun_id)

    def __eq__(self, other):
        return isinstance(other, EDagRun) and self.dagrun_id == other.dagrun_id

    def as_dict(self):
        return dict(
            dag_id=self.dag_id,
            dagrun_id=self.dagrun_id,
            start_date=self.start_date,
            state=self.state,
            end_date=self.end_date,
            execution_date=self.execution_date,
            task_args=self.task_args,
        )


class EDag(object):
    def __init__(
        self,
        description,
        root_task_ids,
        tasks,
        owner,
        dag_id,
        schedule_interval,
        catchup,
        start_date,
        end_date,
        dag_folder,
        hostname,
        source_code,
        is_subdag,
        task_type,
        task_args,
        is_active,
        is_paused,
        git_commit,
        is_committed,
    ):
        self.description = description
        self.root_task_ids = root_task_ids  # type: List[str]
        self.tasks = tasks  # type: List[ETask]
        self.owner = owner
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.start_date = start_date
        self.end_date = end_date
        self.dag_folder = dag_folder
        self.hostname = hostname
        self.source_code = source_code
        self.is_subdag = is_subdag
        self.task_type = task_type
        self.task_args = task_args
        self.is_active = is_active
        self.is_paused = is_paused
        self.git_commit = git_commit
        self.is_committed = is_committed

    @staticmethod
    def from_dag(dag, dm, dag_folder, include_task_args, git_commit, is_committed):
        # type: (DAG, DagModel, str, bool, str, bool) -> EDag
        # Can be Dag from DagBag or from DB, therefore not all attributes may exist
        return EDag(
            description=dag.description or "",
            root_task_ids=[t.task_id for t in getattr(dag, "roots", [])],
            tasks=[
                ETask.from_task(t, include_task_args) for t in getattr(dag, "tasks", [])
            ],
            owner=resolve_attribute_or_default_attribute(dag, ["owner", "owners"]),
            dag_id=dag.dag_id,
            schedule_interval=interval_to_str(dag.schedule_interval),
            catchup=resolve_attribute_or_default_value(dag, "catchup", False),
            start_date=resolve_attribute_or_default_value(dag, "start_date", None),
            end_date=resolve_attribute_or_default_value(dag, "end_date", None),
            dag_folder=dag_folder,
            hostname=get_hostname(),
            source_code=_read_dag_file(dag.fileloc),
            is_subdag=dag.is_subdag,
            task_type="DAG",
            task_args=_extract_args_from_dict(vars(dag)) if include_task_args else {},
            is_active=dm.is_active,
            is_paused=dm.is_paused,
            git_commit=git_commit,
            is_committed=is_committed,
        )

    def as_dict(self):
        return dict(
            description=self.description,
            root_task_ids=self.root_task_ids,
            tasks=[t.as_dict() for t in self.tasks],
            owner=self.owner,
            dag_id=self.dag_id,
            schedule_interval=self.schedule_interval,
            catchup=self.catchup,
            start_date=self.start_date,
            end_date=self.end_date,
            is_committed=self.is_committed,
            git_commit=self.git_commit,
            dag_folder=self.dag_folder,
            hostname=self.hostname,
            source_code=self.source_code,
            is_subdag=self.is_subdag,
            task_type=self.task_type,
            task_args=self.task_args,
        )


class ExportData(object):
    def __init__(self, since, dags=None, dag_runs=None, task_instances=None):
        self.dags = dags or []  # type: List[EDag]
        self.dag_runs = dag_runs or []  # type: List[EDagRun]
        self.task_instances = task_instances or []  # type: List[ETaskInstance]
        self.since = since  # type: Datetime
        self.airflow_version = airflow_version
        self.dags_path = conf.get("core", "dags_folder")
        self.logs_path = conf.get("core", "base_log_folder")
        self.airflow_export_version = _get_export_plugin_version()
        self.rbac_enabled = conf.get("webserver", "rbac")

    def as_dict(self):
        return dict(
            dags=[x.as_dict() for x in self.dags],
            dag_runs=[x.as_dict() for x in self.dag_runs],
            task_instances=[x.as_dict() for x in self.task_instances],
            since=self.since,
            airflow_version=self.airflow_version,
            dags_path=self.dags_path,
            logs_path=self.logs_path,
            airflow_export_version=self.airflow_export_version,
            rbac_enabled=self.rbac_enabled,
        )


### Helpers ###


def resolve_attribute_or_default_value(obj, attribute, default_value):
    if hasattr(obj, attribute):
        return getattr(obj, attribute)
    return default_value


def resolve_attribute_or_default_attribute(obj, attributes_list, default_value=None):
    for attribute in attributes_list:
        if hasattr(obj, attribute):
            return getattr(obj, attribute)
    return default_value


def interval_to_str(schedule_interval):
    if isinstance(schedule_interval, datetime.timedelta):
        if schedule_interval == datetime.timedelta(days=1):
            return "@daily"
        if schedule_interval == datetime.timedelta(hours=1):
            return "@hourly"
    return str(schedule_interval)


def _get_log(ti, task):
    try:
        ti.task = task
        logger = logging.getLogger("airflow.task")
        task_log_reader = conf.get("core", "task_log_reader")
        handler = next(
            (handler for handler in logger.handlers if handler.name == task_log_reader),
            None,
        )
        logs, metadatas = handler.read(ti, ti._try_number, metadata={})
        if not logs:
            return None
        all_logs = logs[0]
        logs_size = sys.getsizeof(all_logs)
        if logs_size < MAX_LOGS_SIZE_IN_BYTES:
            return all_logs

        diff = logs_size - MAX_LOGS_SIZE_IN_BYTES
        result = all_logs[-diff:] + "... ({} of {})".format(diff, len(all_logs))
        return result
    except Exception as e:
        pass
    finally:
        del ti.task


def _get_git_status(path):
    try:
        from git import Repo

        if os.path.isfile(path):
            path = os.path.dirname(path)

        repo = Repo(path, search_parent_directories=True)
        commit = repo.head.commit.hexsha
        return commit, not repo.is_dirty()
    except Exception as ex:
        return "", False


def _get_source_code(t):
    # type: (BaseOperator) -> str
    # TODO: add other "code" extractions
    # TODO: maybe return it with operator code as well
    try:
        from airflow.operators.python_operator import PythonOperator
        from airflow.operators.bash_operator import BashOperator

        if isinstance(t, PythonOperator):
            import inspect

            return inspect.getsource(t.python_callable)
        elif isinstance(t, BashOperator):
            return t.bash_command
    except Exception as ex:
        pass


def _get_module_code(t):
    # type: (BaseOperator) -> str
    try:
        from airflow.operators.python_operator import PythonOperator

        if isinstance(t, PythonOperator):
            import inspect

            return inspect.getsource(inspect.getmodule(t.python_callable))
    except Exception as ex:
        pass


def _get_command_from_operator(t):
    # type: (BaseOperator) -> str
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator

    if isinstance(t, BashOperator):
        return "bash_command='{bash_command}'".format(bash_command=t.bash_command)
    elif isinstance(t, PythonOperator):
        return "python_callable={func}, op_kwargs={kwrags}".format(
            func=t.python_callable.__name__, kwrags=t.op_kwargs
        )


def _extract_args_from_dict(t_dict):
    # type: (Dict) -> Dict[str]
    try:
        # Return only numeric, bool and string attributes
        res = {}
        for k, v in six.iteritems(t_dict):
            if v is None or isinstance(v, TASK_ARG_TYPES):
                res[k] = v
            elif isinstance(v, list):
                res[k] = [
                    val for val in v if val is None or isinstance(val, TASK_ARG_TYPES)
                ]
            elif isinstance(v, dict):
                res[k] = _extract_args_from_dict(v)
        return res
    except Exception as ex:
        task_id = t_dict.get("task_id") or t_dict.get("_dag_id")
        logging.error("Could not collect task args for %s: %s", task_id, ex)
        return {}


def _read_dag_file(dag_file):
    # TODO: Change implementation when this is done:
    # https://github.com/apache/airflow/pull/7217

    if dag_file and os.path.exists(dag_file):
        with open(dag_file) as file:
            try:
                return file.read()
            except Exception as e:
                pass

    return ""


def _get_export_plugin_version():
    try:
        return pkg_resources.get_distribution("dbnd_airflow_export").version
    except Exception:
        # plugin is probably not installed but "copied" to plugins folder so we cannot know its version
        return None


### Views ###


class ExportDataViewAppBuilder(flask_appbuilder.BaseView):
    endpoint = "data_export_plugin"
    default_view = "export_data"

    @flask_appbuilder.has_access
    @flask_appbuilder.expose("/export_data")
    def export_data(self):
        from airflow.www_rbac.views import dagbag

        return export_data_api(dagbag)


class ExportDataViewAdmin(flask_admin.BaseView):
    def __init__(self, *args, **kwargs):
        super(ExportDataViewAdmin, self).__init__(*args, **kwargs)
        self.endpoint = "data_export_plugin"

    @flask_admin.expose("/")
    @flask_admin.expose("/export_data")
    def export_data(self):
        from airflow.www.views import dagbag

        return export_data_api(dagbag)


@provide_session
def get_airflow_data(
    dagbag,
    since,
    include_logs,
    include_task_args,
    include_xcom,
    dag_ids=None,
    quantity=None,
    incomplete_offset=None,
    session=None,
):
    include_logs = bool(include_logs)
    if since:
        since = pendulum.parse(str(since).replace(" 00:00", "Z"))

    # We monkey patch `get_current` to optimize sql querying
    old_get_current_dag = DagModel.get_current
    try:
        DagModel.get_current = _get_current_dag_model

        if incomplete_offset is not None:
            result = _get_airflow_incomplete_data(
                session=session,
                dagbag=dagbag,
                since=since,
                include_task_args=include_task_args,
                dag_ids=dag_ids,
                incomplete_offset=incomplete_offset,
                quantity=quantity,
            )
        else:
            result = _get_airflow_data(
                dagbag=dagbag,
                since=since,
                include_logs=include_logs,
                include_xcom=include_xcom,
                include_task_args=include_task_args,
                dag_ids=dag_ids,
                quantity=quantity,
                session=session,
            )
    finally:
        DagModel.get_current = old_get_current_dag

    if result:
        result = result.as_dict()

    return result


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        # convert dates and numpy objects in a json serializable format
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def json_response(obj):
    return Response(
        response=json.dumps(obj, indent=4, cls=JsonEncoder),
        status=200,
        mimetype="application/json",
    )


def export_data_api(dagbag):
    since = flask.request.args.get("since")
    include_logs = flask.request.args.get("include_logs")
    include_task_args = flask.request.args.get("include_task_args")
    include_xcom = flask.request.args.get("include_xcom")
    dag_ids = flask.request.args.getlist("dag_ids")
    quantity = flask.request.args.get("fetch_quantity", type=int)
    rbac_enabled = conf.get("webserver", "rbac").lower() == "true"
    incomplete_offset = flask.request.args.get("incomplete_offset", type=int)

    if not since and not include_logs and not dag_ids and not quantity:
        new_since = datetime.datetime.utcnow().replace(
            tzinfo=pendulum.timezone("UTC")
        ) - datetime.timedelta(days=1)
        redirect_url = (
            "ExportDataViewAppBuilder" if rbac_enabled else "data_export_plugin"
        )
        redirect_url += ".export_data"
        return flask.redirect(flask.url_for(redirect_url, since=new_since, code=303))

    # do_update = flask.request.args.get("do_update", "").lower() == "true"
    # verbose = flask.request.args.get("verbose", str(not do_update)).lower() == "true"

    try:
        export_data = get_airflow_data(
            dagbag=dagbag,
            since=since,
            include_logs=include_logs,
            include_task_args=include_task_args,
            include_xcom=include_xcom,
            dag_ids=dag_ids,
            quantity=quantity,
            incomplete_offset=incomplete_offset,
        )
        export_data["metrics"] = {
            "performance": flask.g.perf_metrics,
            "sizes": flask.g.size_metrics,
        }
        logging.info("Performance metrics %s", flask.g.perf_metrics)
    except Exception:
        exception_type, exception, exc_traceback = sys.exc_info()
        message = "".join(traceback.format_tb(exc_traceback))
        message += "{}: {}. ".format(exception_type.__name__, exception)
        logging.error("Exception during data export: \n%s", message)
        export_data = {"error": message}
    return json_response(export_data)


### Plugin ###


class DataExportAirflowPlugin(AirflowPlugin):
    name = "dbnd_airflow_export"
    admin_views = [ExportDataViewAdmin(category="Admin", name="Export Data")]
    appbuilder_views = [
        {"category": "Admin", "name": "Export Data", "view": ExportDataViewAppBuilder()}
    ]


### Experimental API:
try:
    # this import is critical for loading `requires_authentication`
    from airflow import api

    api.load_auth()

    from airflow.www_rbac.api.experimental.endpoints import (
        api_experimental,
        requires_authentication,
    )

    @api_experimental.route("/export_data", methods=["GET"])
    @requires_authentication
    def export_data():
        from airflow.www_rbac.views import dagbag

        return export_data_api(dagbag)


except Exception as e:
    logging.error("Export data could not be added to experimental api: %s", e)
