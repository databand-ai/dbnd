from airflow.plugins_manager import AirflowPlugin
from .views import ExportDataViewAdmin, ExportDataViewAppBuilder

import datetime
import logging
import os

import flask
import flask_appbuilder
import pendulum

from airflow.configuration import conf
from airflow.jobs import BaseJob
from airflow.models import BaseOperator, DagModel, DagRun
from airflow.utils.db import provide_session
from airflow.utils.net import get_hostname
from airflow.utils.timezone import utcnow
from sqlalchemy import or_

import flask_admin


class DataExportAirflowPlugin(AirflowPlugin):
    name = "airflow_data_export_plugin"
    admin_views = [ExportDataViewAdmin(category="Admin", name="Export Data")]
    appbuilder_views = [
        {"category": "Admin", "name": "Export Data", "view": ExportDataViewAppBuilder()}
    ]


try:
    # in dbnd it might be overridden
    from airflow.models import original_TaskInstance as TaskInstance
except Exception:
    from airflow.models import TaskInstance


@provide_session
def do_export_data(dagbag, since, session=None):
    task_instances = _get_task_instances(since, session)
    dagruns = _get_dagruns(since, session)
    if not task_instances and not dagruns:
        return ExportData([], [], [])

    dag_models = _get_dag_models(dagruns.keys() if since else None, session)

    ed = ExportData(
        task_instances=[
            ETaskInstance.from_task_instance(
                ti, job, dagbag.get_dag(ti.dag_id).get_task(ti.task_id)
            )
            for ti, job in task_instances
        ],
        dag_runs=[EDagRun.from_dagrun(dr) for dr in dagruns],
        dags=[
            EDag.from_dag(dagbag.get_dag(dm.dag_id), dagbag.dag_folder)
            for dm in dag_models
        ],
    )

    return ed


def _get_dag_models(dag_ids, session):
    dag_models_query = session.query(DagModel)
    if dag_ids is not None:
        if dag_ids:
            dag_models_query = dag_models_query.filter(DagModel.dag_id.in_(dag_ids))
        else:
            return []

    dag_models = dag_models_query.all()
    return dag_models


def _get_dagruns(since, session):
    dagruns_query = session.query(DagRun)
    if since:
        dagruns_query = dagruns_query.filter(
            or_(DagRun.end_date.is_(None), DagRun.end_date > since)
        )
    return dagruns_query.all()


def _get_task_instances(since, session):
    task_instances_query = session.query(TaskInstance, BaseJob).outerjoin(
        BaseJob, TaskInstance.job_id == BaseJob.id
    )
    if since:
        task_instances_query = task_instances_query.filter(
            or_(
                or_(TaskInstance.end_date.is_(None), TaskInstance.end_date > since),
                BaseJob.latest_heartbeat > since,
            )
        )
    return task_instances_query.all()


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
    ):
        self.upstream_task_ids = list(upstream_task_ids)  # type: List[str]
        self.downstream_task_ids = list(downstream_task_ids)  # type: List[str]
        self.task_type = task_type
        self.task_source_code = task_source_code
        self.task_module_code = task_module_code
        self.dag_id = dag_id
        self.task_id = task_id

    @staticmethod
    def from_task(t):
        # type: (BaseOperator) -> ETask
        return ETask(
            upstream_task_ids=t.upstream_task_ids,
            downstream_task_ids=t.downstream_task_ids,
            task_type=t.task_type,
            task_source_code=_get_source_code(t),
            task_module_code=_get_module_code(t),
            dag_id=t.dag_id,
            task_id=t.task_id,
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
        )


class ETaskInstance(object):
    def __init__(
        self,
        execution_date,
        dag_id,
        state,
        try_number,
        task_id,
        start_date,
        end_date,
        log_body,
    ):
        self.execution_date = execution_date
        self.dag_id = dag_id
        self.state = state
        self.try_number = try_number
        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date
        self.log_body = log_body

    @staticmethod
    def from_task_instance(ti, job, task):
        # type: (TaskInstance, BaseJob, BaseOperator) -> ETaskInstance
        return ETaskInstance(
            execution_date=ti.execution_date,
            dag_id=ti.dag_id,
            state=ti.state,
            try_number=ti._try_number,
            task_id=ti.task_id,
            start_date=ti.start_date,
            end_date=ti.end_date or job.latest_heartbeat,
            log_body=_get_log(ti, task),
        )

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
        )


### Models ###


class EDagRun(object):
    def __init__(self, dag_id, dagrun_id, start_date, state, end_date, execution_date):
        self.dag_id = dag_id
        self.dagrun_id = dagrun_id
        self.start_date = start_date
        self.state = state
        self.end_date = end_date
        self.execution_date = execution_date

    @staticmethod
    def from_dagrun(dr):
        # type: (DagRun) -> EDagRun
        return EDagRun(
            dag_id=dr.dag_id,
            dagrun_id=dr.id,  # ???
            start_date=dr.start_date,
            state=dr.state,
            end_date=dr.end_date,
            execution_date=dr.execution_date,
        )

    def as_dict(self):
        return dict(
            dag_id=self.dag_id,
            dagrun_id=self.dagrun_id,
            start_date=self.start_date,
            state=self.state,
            end_date=self.end_date,
            execution_date=self.execution_date,
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
        is_committed,
        git_commit,
        dag_folder,
        hostname,
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
        self.is_committed = is_committed
        self.git_commit = git_commit
        self.dag_folder = dag_folder
        self.hostname = hostname

    @staticmethod
    def from_dag(dag, dag_folder):
        # type: (DAG, str) -> EDag
        git_commit, git_committed = _get_git_status(dag_folder)
        return EDag(
            description=dag.description,
            root_task_ids=[t.task_id for t in dag.roots],
            tasks=[ETask.from_task(t) for t in dag.tasks],
            owner=dag.owner,
            dag_id=dag.dag_id,
            schedule_interval=interval_to_str(dag.schedule_interval),
            catchup=dag.catchup,
            start_date=dag.start_date or utcnow(),
            end_date=dag.end_date,
            is_committed=git_committed,
            git_commit=git_commit or "",
            dag_folder=dag_folder,
            hostname=get_hostname(),
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
        )


class ExportData(object):
    def __init__(self, dags, dag_runs, task_instances):
        self.dags = dags  # type: List[EDag]
        self.dag_runs = dag_runs  # type: List[EDagRun]
        self.task_instances = task_instances  # type: List[ETaskInstance]
        self.timestamp = utcnow()

    def as_dict(self):
        return dict(
            dags=[x.as_dict() for x in self.dags],
            dag_runs=[x.as_dict() for x in self.dag_runs],
            task_instances=[x.as_dict() for x in self.task_instances],
            timestamp=self.timestamp,
        )


### Helpers ###


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
        return logs[0] if logs else None
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
        return None, False


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


### Views ###


class ExportDataViewAppBuilder(flask_appbuilder.BaseView):
    endpoint = "data_export_plugin"
    default_view = "export_data"

    @flask_appbuilder.expose("/export_data")
    def export_data(self):
        from airflow.www_rbac.utils import json_response
        from airflow.www_rbac.views import dagbag

        return json_response(handle_export_data(dagbag))


class ExportDataViewAdmin(flask_admin.BaseView):
    def __init__(self, *args, **kwargs):
        super(ExportDataViewAdmin, self).__init__(*args, **kwargs)
        self.endpoint = "data_export_plugin"

    @flask_admin.expose("/")
    @flask_admin.expose("/export_data")
    def export_data(self):
        from airflow.www.utils import json_response
        from airflow.www.views import dagbag

        return json_response(handle_export_data(dagbag))


def handle_export_data(dagbag):
    since = flask.request.args.get("since")
    if since:
        since = pendulum.parse(since.replace(" 00:00", "Z"))
    # do_update = flask.request.args.get("do_update", "").lower() == "true"
    # verbose = flask.request.args.get("verbose", str(not do_update)).lower() == "true"

    result = do_export_data(dagbag=dagbag, since=since)

    if result:
        result = result.as_dict()

    return result
