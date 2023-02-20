# © Copyright Databand.ai, an IBM Company 2022

import logging
import time
import traceback


logger = logging.getLogger(__name__)


def remove_listener_by_name(target, identifier, name):
    """
    removes already registered sqlalchemy listener
    use this one only if fn pointer is not accessable (inner function)
    otherwise use regular remove from event api

    Example:
        from airflow import settings
        target = settings.engine
        remove_listener_by_name(target, "engine_connect", "ping_connection")
    """
    import ctypes

    from sqlalchemy import event

    all_keys = list(event.registry._key_to_collection.items())
    for key, values in all_keys:

        if key[0] != id(target):
            continue

        if identifier != key[1]:
            continue

        fn = ctypes.cast(key[2], ctypes.py_object).value  # get function by id
        if fn.__name__ != name:
            continue

        event.remove(target, identifier, fn)


def get_calling_line():
    code = "unknown"
    for (file_path, val1, val2, line_contents) in traceback.extract_stack():
        if "airflow" not in file_path:
            continue
        if (
            "utils/sqlalchemy.py" in file_path
            or "utils/db.py" in file_path
            or "db_utils" in file_path
        ):
            continue
        code = str((file_path, val1, val2))
    return code


def profile_before_cursor_execute(conn, cursor, statement, *_):
    conn.info.setdefault("query_start_time", []).append(time.time())
    logger.debug("Start Query: %s", statement)


def profile_after_cursor_execute(conn, cursor, statement, parameters, *_):
    total = time.time() - conn.info["query_start_time"].pop(-1)
    logger.debug(
        "Query Complete! %s  \n--> %f seconds\nPARAMS: %s", statement, total, parameters
    )


def airflow_tables_to_dump():
    from airflow import jobs as af_jobs, models as af_models

    return (
        # dbnd_dag -> dag_id
        # dbnd_airflow_models.DbndAirflowDagModel,
        # dbnd_dag_run -> user, [cmd_line]
        # dag_run -> dag_id,
        af_models.DagRun,
        af_models.TaskInstance,
        # dbnd_task_run -> created_by_task_id (__XXX), created_by_dag_id, task_name
        # dbnd_task_run_metrics -> name,
        # dag -> dag_id, [fileloc]
        af_models.DagModel,
        # job -> dag_id, hostname, unixname
        af_jobs.BaseJob,
    )


def airflow_sql_conn_url():
    try:
        from sqlalchemy.engine.url import make_url
    except:
        return "`pip install sqlalchemy` in order to get sql db url"

    return repr(make_url(airlow_sql_alchemy_conn()))


def airlow_sql_alchemy_conn():
    from airflow.configuration import conf as airflow_conf

    return airflow_conf.get("core", "sql_alchemy_conn")
