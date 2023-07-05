# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from functools import partial
from typing import Dict, List

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.constants import RunState, TaskRunState, UpdateSource, _TaskDbndRun
from dbnd._core.context.use_dbnd_airflow_tracking import should_use_airflow_monitor
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.tracking.schemas.tracking_info_objects import (
    TargetInfo,
    TaskDefinitionInfo,
    TaskRunInfo,
    TaskRunParamInfo,
)
from dbnd._core.tracking.schemas.tracking_info_run import RunInfo
from dbnd._core.utils.string_utils import safe_short_string
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.traversing import traverse
from dbnd._core.utils.uid_utils import source_md5
from dbnd.api.tracking_api import InitRunArgs, TaskRunsInfo


if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task_run.task_run import TaskRun
    from targets import Target

logger = logging.getLogger(__name__)


class TrackingInfoBuilder(object):
    def __init__(self, run):
        self.run = run  # type: DatabandRun

    def _run_to_run_info(self):
        # type: () -> RunInfo
        run = self.run
        run_executor = run.run_executor
        task = run.driver_task_run.task
        context = run.context
        if run_executor:
            env = run_executor.env
            env_name = env.name
            env_cloud_type = env.cloud_type
            driver_name = env.remote_engine or env.local_engine

            sends_heartbeat = run_executor.send_heartbeat
            task_executor = run_executor.task_executor_type
        else:
            env_name = "local"
            env_cloud_type = "local"
            driver_name = "local"

            sends_heartbeat = False
            task_executor = ""

        return RunInfo(
            run_uid=run.run_uid,
            job_name=run.job_name,
            project_name=run.project_name,
            user=context.task_run_env.user,
            name=run.name,
            state=RunState.RUNNING,
            start_time=utcnow(),
            end_time=None,
            description=run.description,
            is_archived=run.is_archived,
            env_name=env_name,
            cloud_type=env_cloud_type,
            # deprecate and airflow
            dag_id=run.dag_id,
            execution_date=run.execution_date,
            cmd_name=context.name,
            driver_name=driver_name,
            # move to task
            target_date=task.task_target_date,
            version=task.task_version,
            # root and submitted by
            root_run=run.root_run_info,
            scheduled_run=run.scheduled_run_info,
            trigger="unknown",
            sends_heartbeat=sends_heartbeat,
            task_executor=task_executor,
        )

    def build_init_args(self):
        # type: () -> InitRunArgs

        run = self.run
        task_run_info = self.build_task_runs_info(run.task_runs)
        init_args = InitRunArgs(
            run_uid=run.run_uid,
            root_run_uid=run.root_run_info.root_run_uid,
            task_runs_info=task_run_info,
            driver_task_uid=run.driver_task_run.task_run_uid,
            task_run_env=run.context.task_run_env,
            source=run.source,
            af_with_monitor=should_use_airflow_monitor(),
            af_context=run.af_context,
            tracking_source=run.tracking_source,
        )

        if (
            not run.existing_run
            or get_dbnd_project_config().resubmit_run
            or run.source == UpdateSource.airflow_tracking
        ):
            # even if it's existing run, may be we are running from Airflow
            # so the run is actually "submitted", ( the root airflow job has no info..,
            # we want to capture "real" info of the run
            init_args.new_run_info = self._run_to_run_info()

        if run.scheduled_run_info:
            init_args.scheduled_run_info = run.scheduled_run_info

        if run.root_run_info.root_task_run_uid:
            rel = (run.root_run_info.root_task_run_uid, init_args.driver_task_uid)
            task_run_info.parent_child_map.add(rel)
            task_run_info.upstreams_map.add(rel)

        return init_args

    def build_task_runs_info(self, task_runs, dynamic_task_run_update=False):
        # type: (List[TaskRun], bool) -> TaskRunsInfo
        run = self.run
        task_defs = {}
        all_task_models = {}
        all_targets = {}
        for task_run in task_runs:
            task = task_run.task
            # we process only tasks in current dag
            task_def_id = task.task_definition.full_task_family
            if task_def_id not in task_defs:
                task_defs[task_def_id] = task_to_task_def(run.context, task)

            self.task_to_targets(task, all_targets)
            all_task_models[task.task_id] = build_task_run_info(task_run)

        def _add_rel(rel_map, t_id_1, t_id_2):
            if t_id_1 in all_task_models or t_id_2 in all_task_models:
                tr_1 = run.get_task_run_by_id(t_id_1)
                tr_2 = run.get_task_run_by_id(t_id_2)
                if tr_1 and tr_2:
                    rel_map.add((tr_1.task_run_uid, tr_2.task_run_uid))

        # set children/upstreams maps
        upstreams_map = set()
        parent_child_map = set()

        for task_run in run.task_runs:
            task = task_run.task
            for t_id in task.descendants.children:
                _add_rel(parent_child_map, task.task_id, t_id)

            task_dag = task.ctrl.task_dag
            for upstream in task_dag.upstream:
                _add_rel(upstreams_map, task.task_id, upstream.task_id)

        return TaskRunsInfo(
            run_uid=self.run.run_uid,
            root_run_uid=self.run.root_run_info.root_run_uid,
            task_run_env_uid=run.context.task_run_env.uid,
            task_definitions=[val for key, val in sorted(task_defs.items())],
            task_runs=[val for key, val in sorted(all_task_models.items())],
            targets=[val for key, val in sorted(all_targets.items())],
            parent_child_map=parent_child_map,
            upstreams_map=upstreams_map,
            dynamic_task_run_update=dynamic_task_run_update,
            af_context=run.af_context,
            parent_task_run_uid=run.root_run_info.root_task_run_uid,
            parent_task_run_attempt_uid=run.root_run_info.root_task_run_attempt_uid,
        )

    def task_to_targets(
        self, task: "_BaseTask", targets: Dict[str, "TargetInfo"]
    ) -> List[TargetInfo]:
        """
        :param task:
        :param targets: all known targets for current run, so we have uniq list of targets (by path)
        :return:
        """

        run = self.run
        task_targets = []

        def process_target(target: "Target", name: str):
            target_path = str(target)
            dbnd_target = targets.get(target_path)
            if not dbnd_target:
                # we see this target for the first time
                target_task_run_uid = (
                    None  # let assume that Target is now owned by any task
                )
                # let try to find it's owner, so we create target that relates to some Task
                # if `task` is pipeline, the target owner is going to be different task
                if target.task:
                    target_task_run = run.get_task_run(target.task.task_id)
                    if target_task_run:
                        target_task_run_uid = target_task_run.task_run_uid

                dbnd_target = targets[target_path] = TargetInfo(
                    path=target_path,
                    created_date=utcnow(),
                    task_run_uid=target_task_run_uid,
                    parameter_name=name,
                )
                logger.debug(
                    "New Target: %s -> %s ->  %s",
                    target.task,
                    target_task_run_uid,
                    target_path,
                )
            task_targets.append(dbnd_target)

        for io_params in task.ctrl.io_params():
            for name, t in io_params.items():
                traverse(t, convert_f=partial(process_target, name=name))

        return task_targets


def task_to_task_def(ctx: "DatabandContext", task: _BaseTask) -> TaskDefinitionInfo:
    td = task.task_definition

    task_param_definitions = [
        value for key, value in sorted(td.task_param_defs.items())
    ]
    task_family = task.task_family
    source_code = td.source_code
    task_definition = TaskDefinitionInfo(
        task_definition_uid=td.task_definition_uid,
        class_version=task.task_class_version,
        family=task_family,
        module_source=source_code.task_module_code,
        module_source_hash=source_md5(source_code.task_module_code),
        name=task_family,
        source=source_code.task_source_code,
        source_hash=source_md5(source_code.task_source_code),
        type=task.task_type,
        task_param_definitions=task_param_definitions,
    )
    return task_definition


def _get_path(value):
    if value:
        return value.path
    return None


def build_task_run_info(task_run):
    # type: (TaskRun) -> TaskRunInfo
    t = task_run.task
    task_dag = t.ctrl.task_dag

    if task_run.task_run_executor:
        log_manager = task_run.task_run_executor.log_manager
        log_local, log_remote = _get_path(log_manager.local_log_file), _get_path(
            log_manager.remote_log_file
        )
    else:
        log_local, log_remote = None, None

    task_run_params = []
    for param_meta in t.task_params.get_param_values():
        if isinstance(param_meta.parameter, FuncResultParameter):
            continue

        if param_meta:
            value_source, value = param_meta.source, param_meta.value
            param_meta.parameter = (
                param_meta.parameter.update_value_meta_conf_from_runtime_value(
                    value, t.settings.tracking
                )
            )
        else:
            value_source, value = "", ""

        if param_meta.parameter.hidden:
            value = "***"
        elif not param_meta.parameter.value_meta_conf.log_preview:
            value = None
        else:
            value = safe_short_string(
                param_meta.parameter.signature(value), max_value_len=5000
            )
        task_run_params.append(
            TaskRunParamInfo(
                parameter_name=param_meta.name,
                value_origin=safe_short_string(str(value_source), max_value_len=5000),
                value=value,
            )
        )

    kwargs = {}
    if isinstance(t, _TaskDbndRun):
        kwargs.update(
            {
                "command_line": t.ctrl.task_repr.task_command_line,
                "functional_call": t.ctrl.task_repr.task_functional_call,
                "env": t.task_env.name if t.task_env else "local",
            }
        )
    else:
        kwargs.update({"command_line": "", "functional_call": "", "env": ""})

    return TaskRunInfo(
        run_uid=task_run.run.run_uid,
        task_definition_uid=task_run.task.task_definition.task_definition_uid,
        task_run_uid=task_run.task_run_uid,  # this is not the TaskRun uid
        task_run_attempt_uid=task_run.task_run_attempt_uid,  # this is not the TaskRun uid
        task_id=t.task_id,
        task_af_id=task_run.task_af_id,
        name=t.task_name,
        task_signature=t.task_signature_obj.signature,
        task_signature_source=t.task_signature_obj.signature_source,
        output_signature=t.task_outputs_signature_obj.signature,
        has_downstreams=bool(task_dag.downstream),
        has_upstreams=bool(task_dag.upstream),
        state=TaskRunState.SCHEDULED
        if not task_run.is_reused
        else TaskRunState.SUCCESS,
        is_reused=task_run.is_reused,
        is_skipped=task_run.is_skipped,
        is_dynamic=task_run.is_dynamic,
        is_system=task_run.is_system,
        version=t.task_version,
        target_date=t.task_target_date,
        log_local=log_local,
        log_remote=log_remote,
        task_run_params=task_run_params,
        execution_date=task_run.run.execution_date,
        is_root=task_run.is_root,
        **kwargs
    )
