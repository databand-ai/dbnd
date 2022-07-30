# © Copyright Databand.ai, an IBM Company 2022

import collections
import json
import logging

from functools import wraps

from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, DagModel
from airflow.utils.db import provide_session
from airflow.version import version as airflow_version

import dbnd_airflow

from dbnd._core.utils.uid_utils import get_airflow_instance_uid
from dbnd_airflow.export_plugin.compat import get_api_mode
from dbnd_airflow.export_plugin.dag_operations import (
    get_current_dag_model,
    get_dags,
    load_dags_models,
)
from dbnd_airflow.export_plugin.metrics import METRIC_COLLECTOR
from dbnd_airflow.export_plugin.models import (
    AirflowExportData,
    AirflowExportMeta,
    AirflowNewDagRun,
    DagRunsStatesData,
    FullRunsData,
    LastSeenData,
    NewRunsData,
)
from dbnd_airflow.export_plugin.queries import (
    find_all_logs_grouped_by_runs,
    find_full_dag_runs,
    find_max_dag_run_id,
    find_max_log_run_id,
    find_new_dag_runs,
)
from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader
from dbnd_airflow.export_plugin.utils import AIRFLOW_VERSION_2


logger = logging.getLogger(__name__)


DATABAND_AIRFLOW_CONN_ID = "dbnd_config"


def safe_rich_result(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        with METRIC_COLLECTOR.use_local() as metrics:
            result = AirflowExportData()
            try:
                result = f(*args, **kwargs)
            except Exception as e:
                result = AirflowExportData()
                logger.exception("Exception during %s", f.__name__, exc_info=True)
                result.error_message = str(e)
            finally:
                result.airflow_export_meta = get_meta(metrics)
            return result

    return wrapped


def get_meta(metrics):
    import flask

    meta = AirflowExportMeta(
        airflow_version=airflow_version,
        plugin_version=dbnd_airflow.__version__,
        airflow_instance_uid=get_airflow_instance_uid(),
        api_mode=get_api_mode(),
        request_args=dict(flask.request.args) if flask.has_request_context() else {},
        metrics={
            "performance": metrics.get("perf_metrics", {}),
            "sizes": metrics.get("size_metrics", {}),
        },
    )
    return meta


@safe_rich_result
@provide_session
def get_last_seen_values(session=None):
    max_dag_run_id = find_max_dag_run_id(session)
    max_log_id = find_max_log_run_id(session)

    return LastSeenData(
        last_seen_dag_run_id=max_dag_run_id, last_seen_log_id=max_log_id
    )


@safe_rich_result
@provide_session
def get_new_dag_runs(
    last_seen_dag_run_id,
    last_seen_log_id,
    extra_dag_runs_ids,
    dag_ids=None,
    include_subdags=True,
    session=None,
):
    max_dag_run_id = find_max_dag_run_id(session)
    max_log_id = find_max_log_run_id(session)

    if last_seen_dag_run_id is None:
        last_seen_dag_run_id = max_dag_run_id

    if last_seen_log_id is None:
        last_seen_log_id = max_log_id

    logs = find_all_logs_grouped_by_runs(last_seen_log_id, dag_ids, session)
    logs_dict = {(log.dag_id, log.execution_date): log for log in logs}

    dag_runs = find_new_dag_runs(
        last_seen_dag_run_id,
        extra_dag_runs_ids,
        logs_dict.keys(),
        dag_ids,
        include_subdags,
        session,
    )

    new_dag_runs = []
    for dag_run in dag_runs:
        log = logs_dict.get((dag_run.dag_id, dag_run.execution_date), None)

        if log is None:
            events = []
        elif isinstance(log.events, str):  # mysql, sqlite
            events = log.events.split(",")
        else:  # postgres
            events = log.events

        new_dag_run = AirflowNewDagRun(
            id=dag_run.id,
            dag_id=dag_run.dag_id,
            execution_date=dag_run.execution_date,
            state=dag_run.state,
            is_paused=dag_run.is_paused,
            has_updated_task_instances=log is not None,
            events=events,
            max_log_id=log.id if log else None,
        )
        new_dag_runs.append(new_dag_run)

    new_runs = NewRunsData(
        new_dag_runs=new_dag_runs,
        last_seen_dag_run_id=max_dag_run_id,
        last_seen_log_id=max_log_id,
    )
    return new_runs


@safe_rich_result
@provide_session
def get_full_dag_runs_for_plugin(dag_run_ids, include_sources, session=None):
    dbnd_dag_loader = DbndDagLoader()
    if AIRFLOW_VERSION_2:
        dbnd_dag_loader.load_dags_for_runs(dag_run_ids=dag_run_ids, session=session)
    else:
        import airflow

        if airflow.settings.RBAC:
            from airflow.www_rbac.views import dagbag
        else:
            from airflow.www.views import dagbag
        # this is preloaded dagbag, we are in UI context, dagbag is global variable which is loaded
        # we "load" all dags from the dagbag directly into DagLoader
        dbnd_dag_loader.load_from_dag_bag(dagbag)

    return get_full_dag_runs(
        dag_run_ids=dag_run_ids,
        include_sources=include_sources,
        dag_loader=dbnd_dag_loader,
    )


@safe_rich_result
@provide_session
def get_full_dag_runs(dag_run_ids, include_sources, dag_loader, session=None):

    old_get_current_dag = DagModel.get_current
    try:
        DagModel.get_current = get_current_dag_model
        load_dags_models(session)
        task_instances, dag_runs = find_full_dag_runs(dag_run_ids, session)
        dag_ids = set(run.dag_id for run in dag_runs)
        dags = get_dags(dag_loader, True, dag_ids, False, include_sources)
        full_runs = FullRunsData(
            task_instances=task_instances, dag_runs=dag_runs, dags=dags
        )
        return full_runs
    finally:
        DagModel.get_current = old_get_current_dag


@safe_rich_result
@provide_session
def get_dag_runs_states_data(dag_run_ids, session=None):
    task_instances, dag_runs = find_full_dag_runs(dag_run_ids, session)
    dag_runs_states_data = DagRunsStatesData(
        task_instances=task_instances, dag_runs=dag_runs
    )
    return dag_runs_states_data


def deep_update(source, overrides):
    """
    Update a nested dictionary or similar mapping.
    Modify ``source`` in place.
    """
    for key, value in overrides.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = overrides[key]
    return source


def remove_place_holders(dbnd_response):
    """Remove from configurations value which only have placeholders, so they won't override valid values."""
    if dbnd_response.get("core") is not None:
        dbnd_response["core"].pop("databand_access_token", None)
        dbnd_response["core"].pop("databand_url", None)
        if not dbnd_response["core"]:
            dbnd_response.pop("core", None)


def get_or_create_db_connection(airflow_connection_from_hook, session):
    existing_db_connection = (
        session.query(Connection)
        .filter(Connection.conn_id == DATABAND_AIRFLOW_CONN_ID)
        .first()
    )

    if existing_db_connection:
        return existing_db_connection

    session.add(airflow_connection_from_hook)
    return airflow_connection_from_hook


@safe_rich_result
@provide_session
def check_syncer_config_and_set(dbnd_response, session=None):
    airflow_connection_from_hook = BaseHook.get_connection(DATABAND_AIRFLOW_CONN_ID)
    airflow_response = airflow_connection_from_hook.extra_dejson
    remove_place_holders(dbnd_response)

    deep_update(airflow_response, dbnd_response)

    # A connection in Airflow can be set from the UI (saved in the DB) or from an environment variable (not in the db)
    airflow_connection = get_or_create_db_connection(
        airflow_connection_from_hook, session
    )
    airflow_connection.set_extra(json.dumps(airflow_response, indent=True))

    session.commit()

    return AirflowExportData()
