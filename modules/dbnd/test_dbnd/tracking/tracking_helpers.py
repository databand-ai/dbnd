from collections import defaultdict
from typing import Iterable, Set, Tuple

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


def get_task_runs_info(mock_channel_tracker):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0].__name__ == "init_run":
            yield call[1]["init_args"].task_runs_info
        elif call.args[0].__name__ == "add_task_runs":
            yield call[1]["task_runs_info"]


def get_log_targets(mock_channel_tracker):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0].__name__ == "log_targets":
            for target_info in call[1]["targets_info"]:
                yield target_info


def get_log_metrics(mock_channel_tracker):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0].__name__ == "log_metrics":
            for metric_info in call[1]["metrics_info"]:
                yield metric_info


def get_reported_params(mock_channel_tracker):
    param_definitions = defaultdict(list)
    run_time_params = defaultdict(list)
    for task_runs_info in get_task_runs_info(mock_channel_tracker):
        for task_definition_info in task_runs_info.task_definitions:
            param_definitions[task_definition_info.name].extend(
                task_definition_info.task_param_definitions
            )

        for task_run_info in task_runs_info.task_runs:
            run_time_params[task_run_info.name].extend(task_run_info.task_run_params)

    return param_definitions, run_time_params
