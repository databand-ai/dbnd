import logging

import flask

from airflow.version import version as airflow_version

from dbnd_airflow_export.api_functions import (
    get_dag_runs_states_data,
    get_full_dag_runs,
    get_last_seen_values,
    get_new_dag_runs,
)
from dbnd_airflow_export.models import AirflowExportData, AirflowExportMeta
from dbnd_airflow_export.utils import json_response


def process_last_seen_values_request():
    result = AirflowExportData()
    try:
        result = get_last_seen_values()
    except Exception as e:
        result = AirflowExportData()
        logging.error("Exception during data export: \n%s", str(e))
        result.error_message = str(e)
    finally:
        result.airflow_export_meta = get_meta()
        return json_response(result.as_dict())


def process_new_runs_request():
    last_seen_dag_run_id = flask.request.args.get("last_seen_dag_run_id", type=int)
    last_seen_log_id = flask.request.args.get("last_seen_log_id", type=int)
    extra_dag_runs_ids = (
        list(
            map(int, flask.request.args.get("extra_dag_runs_ids", type=str).split(","),)
        )
        if "extra_dag_runs_ids" in flask.request.args
        else []
    )

    result = AirflowExportData()
    try:
        result = get_new_dag_runs(
            last_seen_dag_run_id, last_seen_log_id, extra_dag_runs_ids
        )
    except Exception as e:
        result = AirflowExportData()
        logging.error("Exception during data export: \n%s", str(e))
        result.error_message = str(e)
    finally:
        result.airflow_export_meta = get_meta()
        return json_response(result.as_dict())


def process_full_runs_request():
    dag_run_ids = (
        list(map(int, flask.request.args.get("dag_run_ids", type=str).split(",")))
        if "dag_run_ids" in flask.request.args
        else []
    )

    result = AirflowExportData()
    try:
        result = get_full_dag_runs(dag_run_ids)
    except Exception as e:
        result = AirflowExportData()
        logging.error("Exception during data export: \n%s", str(e))
        result.error_message = str(e)
    finally:
        result.airflow_export_meta = get_meta()
        return json_response(result.as_dict())


def process_dag_run_states_data_request():
    dag_run_ids = (
        list(map(int, flask.request.args.get("dag_run_ids", type=str).split(",")))
        if "dag_run_ids" in flask.request.args
        else []
    )

    result = AirflowExportData()
    try:
        result = get_dag_runs_states_data(dag_run_ids)
    except Exception as e:
        result = AirflowExportData()
        logging.error("Exception during data export: \n%s", str(e))
        result.error_message = str(e)
    finally:
        result.airflow_export_meta = get_meta()
        return json_response(result.as_dict())


def get_meta():
    meta = AirflowExportMeta()
    meta.airflow_version = airflow_version
    meta.plugin_version = "2.0"
    meta.request_args = flask.request.args

    metrics = {}
    if hasattr(flask.g, "perf_metrics"):
        metrics["performance"] = flask.g.perf_metrics
    if hasattr(flask.g, "sizes"):
        metrics["sizes"]: flask.g.size_metrics

    meta.metrics = metrics

    return meta
