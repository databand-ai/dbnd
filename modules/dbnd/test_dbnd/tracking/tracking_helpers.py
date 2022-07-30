# © Copyright Databand.ai, an IBM Company 2022

from collections import defaultdict
from typing import Dict, Iterable, List, Set, Tuple

from dbnd._core.utils.dotdict import rdotdict
from dbnd.api.tracking_api import TaskRunsInfo


def build_graph_from_calls(mock_channel_tracker):
    multiple_task_runs_info = get_task_runs_info(mock_channel_tracker)
    return build_tasks_connection_from_runs_info(multiple_task_runs_info)


def build_tasks_connection_from_runs_info(multiple_task_runs_info):
    # type: (Iterable[TaskRunsInfo]) -> Tuple[Set[Tuple[str, str]],Set[Tuple[str, str]]]
    task_id_to_name = {}
    child_connections = set()
    downstream_connections = set()
    for task_runs_info in multiple_task_runs_info:
        for task_run in task_runs_info.task_runs:
            task_id_to_name[task_run.task_run_uid] = task_run.name

        for task_id, upstream_task_id in task_runs_info.upstreams_map:
            downstream_connections.add(
                (task_id_to_name[upstream_task_id], task_id_to_name[task_id])
            )

        for task_id, child in task_runs_info.parent_child_map:
            child_connections.add((task_id_to_name[task_id], task_id_to_name[child]))

    return child_connections, downstream_connections


def get_call_args(mock_channel_tracker, calls: List[str]):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0] in calls:
            # we use rdotdict() here because many tests was written when it was possible
            # to use dot-notation to access data of channel-tracker calls, as data was
            # catched before convertion to json. This probably should be removed one day.
            yield call.args[0], rdotdict.try_create(call.args[1])


def get_task_runs_info(mock_channel_tracker):
    for name, data in get_call_args(
        mock_channel_tracker, ["init_run", "add_task_runs"]
    ):
        if name == "init_run":
            yield data["init_args"].task_runs_info
        else:
            yield data["task_runs_info"]


def get_log_targets(mock_channel_tracker):
    for _, data in get_call_args(mock_channel_tracker, ["log_targets"]):
        for target_info in data["targets_info"]:
            yield target_info


def get_log_datasets(mock_channel_tracker):
    for _, data in get_call_args(mock_channel_tracker, ["log_datasets"]):
        for dataset_info in data["datasets_info"]:
            yield dataset_info


def get_log_metrics(mock_channel_tracker):
    for _, data in get_call_args(mock_channel_tracker, ["log_metrics"]):
        for metric_info in data["metrics_info"]:
            yield metric_info


def get_task_target_result(mock_channel_tracker, task_name):
    for target_info in get_log_targets(mock_channel_tracker):
        if (
            target_info.task_run_name == task_name
            and target_info.param_name == "result"
        ):
            return target_info
    return None


def get_task_multi_target_result(mock_channel_tracker, task_name, names):
    return {
        target_info.param_name: target_info
        for target_info in get_log_targets(mock_channel_tracker)
        if target_info.task_run_name == task_name and target_info.param_name in names
    }


def get_reported_params(mock_channel_tracker, task_name=None):
    # type: (...) -> Tuple[Dict, Dict, Dict]
    param_definitions = defaultdict(dict)
    run_time_params = defaultdict(dict)
    param_definition_task_def_uid = defaultdict(dict)
    for task_runs_info in get_task_runs_info(mock_channel_tracker):
        for task_definition_info in task_runs_info.task_definitions:
            param_definition_task_def_uid[task_definition_info.name].update(
                {
                    p.name: task_definition_info["task_definition_uid"]
                    for p in task_definition_info.task_param_definitions
                }
            )
            param_definitions[task_definition_info.name].update(
                {p.name: p for p in task_definition_info.task_param_definitions}
            )

        for task_run_info in task_runs_info.task_runs:
            run_time_params[task_run_info.name].update(
                {p.parameter_name: p for p in task_run_info.task_run_params}
            )

    if task_name:
        return (
            param_definitions[task_name],
            run_time_params[task_name],
            param_definition_task_def_uid[task_name],
        )
    return param_definitions, run_time_params, param_definition_task_def_uid


def get_save_external_links(mock_channel_tracker):
    for _, data in get_call_args(mock_channel_tracker, ["save_external_links"]):
        yield data


def get_reported_source_code(mock_channel_tracker):
    task_source_code = defaultdict(dict)
    for task_runs_info in get_task_runs_info(mock_channel_tracker):
        task_source_code = task_runs_info.task_definitions[0].source
    return task_source_code
