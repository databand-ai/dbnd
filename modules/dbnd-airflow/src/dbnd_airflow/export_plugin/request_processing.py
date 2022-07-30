# © Copyright Databand.ai, an IBM Company 2022

import flask

from dbnd_airflow.export_plugin.api_functions import (
    get_dag_runs_states_data,
    get_full_dag_runs_for_plugin,
    get_last_seen_values,
    get_meta,
    get_new_dag_runs,
)
from dbnd_airflow.export_plugin.utils import json_response


def convert_url_param_value_to_list(
    param_name, value_type, default_value, separator=","
):
    if param_name not in flask.request.values:
        return default_value

    param_value = flask.request.values.get(param_name, type=str)
    if not param_value:
        return default_value

    return list(map(value_type, param_value.split(separator)))


def process_metadata_request():
    return json_response(get_meta({}).as_dict())


def process_last_seen_values_request():
    return json_response(get_last_seen_values().as_dict())


def process_new_runs_request():
    last_seen_dag_run_id = flask.request.values.get("last_seen_dag_run_id", type=int)
    last_seen_log_id = flask.request.values.get("last_seen_log_id", type=int)
    extra_dag_runs_ids = convert_url_param_value_to_list("extra_dag_runs_ids", int, [])
    dag_ids = convert_url_param_value_to_list("dag_ids", str, None)
    # default to true
    include_subdags = flask.request.values.get("include_subdags", "").lower() != "false"

    return json_response(
        get_new_dag_runs(
            last_seen_dag_run_id,
            last_seen_log_id,
            extra_dag_runs_ids,
            dag_ids,
            include_subdags,
        ).as_dict()
    )


def process_full_runs_request():
    dag_run_ids = convert_url_param_value_to_list("dag_run_ids", int, [])
    include_sources = flask.request.values.get("include_sources", "").lower() == "true"

    return json_response(
        get_full_dag_runs_for_plugin(dag_run_ids, include_sources).as_dict()
    )


def process_dag_run_states_data_request():
    dag_run_ids = convert_url_param_value_to_list("dag_run_ids", int, [])

    return json_response(get_dag_runs_states_data(dag_run_ids).as_dict())
