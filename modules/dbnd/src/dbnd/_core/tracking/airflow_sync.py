import logging
import uuid
from collections import defaultdict
from typing import Dict

from airflow.utils.state import State
from marshmallow import fields, post_load
from sqlalchemy.exc import IntegrityError

import attr

from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.tracking.tracking_info_convertor import source_md5
from dbnd._core.tracking.tracking_info_objects import TaskDefinitionInfo, TaskRunEnvInfo, TaskRunInfo
from dbnd._core.tracking.tracking_info_run import RunInfo, RootRunInfo, ScheduledRunInfo
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_uuid
from dbnd._vendor.namesgenerator import get_random_name
from dbnd.api.api_utils import ApiObjectSchema
from dbnd.api.tracking_api import (
    InitRunArgsSchema,
    SaveTaskRunLogSchema,
    ScheduledJobInfoSchema,
    SetDatabandRunStateSchema,
    TaskRunAttemptUpdateArgsSchema,
    ScheduledJobInfo, TaskRunsInfo, InitRunArgs, TaskRunAttemptUpdateArgs)

logger = logging.getLogger(__name__)

NAMESPACE_DBND = uuid.uuid5(uuid.NAMESPACE_DNS, "databand.ai")
NAMESPACE_DBND_JOB = uuid.uuid5(NAMESPACE_DBND, "job")
NAMESPACE_DBND_RUN = uuid.uuid5(NAMESPACE_DBND, "run")
NAMESPACE_DBND_TASK_DEF = uuid.uuid5(NAMESPACE_DBND, "task_definition")


@attr.s
class ExportDataV1(object):
    init_args = attr.ib(factory=list)
    task_run_attempt_updates = attr.ib(factory=list)
    scheduled_job_infos = attr.ib(factory=list)
    updated_runs = attr.ib(factory=list)
    logs = attr.ib(factory=list)  # type: List[SaveTaskRunLog]
    run_states = attr.ib(factory=list)  # type: List[SetRunStateArgs]
    timestamp = attr.ib(factory=utcnow)


class ExportDataSchema(ApiObjectSchema):
    init_args = fields.Nested(InitRunArgsSchema, many=True)
    task_run_attempt_updates = fields.Nested(TaskRunAttemptUpdateArgsSchema, many=True)
    scheduled_job_infos = fields.Nested(ScheduledJobInfoSchema, many=True)
    logs = fields.Nested(SaveTaskRunLogSchema, many=True)
    run_states = fields.Nested(SetDatabandRunStateSchema, many=True)
    updated_runs = fields.List(fields.List(fields.String()))
    timestamp = fields.DateTime()

    short_response_fields = ("updated_runs", "timestamp")

    @post_load
    def make_init_run_args(self, data, **kwargs):
        # we need this while SaveTaskRunLog is not created automatically from SaveTaskRunLogSchema
        if data.get("logs"):
            data["logs"] = [SaveTaskRunLog(**i) for i in data["logs"]]
        # we need those while SetRunStateArgs is not created automatically from SetDatabandRunStateSchema
        if data.get("run_states"):
            data["run_states"] = [SetRunStateArgs(**i) for i in data["run_states"]]
        return ExportDataV1(**data)


@attr.s
class SaveTaskRunLog(object):
    task_run_attempt_uid = attr.ib()
    log_body = attr.ib()


@attr.s
class SetRunStateArgs(object):
    run_uid = attr.ib()
    state = attr.ib()
    timestamp = attr.ib()


def do_import_data(result):
    # type: (ExportDataV1) -> None
    from dbnd_web.services.tracking_db_service import TrackingDbService

    tracking_service = TrackingDbService()
    for scheduled_job in result.scheduled_job_infos:
        try:
            tracking_service.init_scheduled_job(scheduled_job)
        except Exception as e:
            log_f = logger.exception
            if isinstance(e, IntegrityError) and "UNIQUE" in str(e):
                # assuming already exists
                log_f = logger.warning
            log_f("Failed init_scheduled_job for {}".format(scheduled_job.name))

    for init_args in result.init_args:
        try:
            tracking_service.init_run(init_args)
        except Exception as e:
            log_f = logger.exception
            if isinstance(e, IntegrityError) and "UNIQUE" in str(e):
                # assuming already exists
                log_f = logger.warning
            log_f("Failed init_run for {}".format(init_args.new_run_info))

    # for task_run_attempt_updates in all_updates:
    tracking_service.update_task_run_attempts(result.task_run_attempt_updates)

    for log in result.logs:  # type: SaveTaskRunLog
        tracking_service.save_task_run_log(
            task_run_attempt_uid=log.task_run_attempt_uid, log_body=log.log_body
        )

    for run_state in result.run_states:  # type: SetRunStateArgs
        tracking_service.set_run_state(
            run_uid=run_state.run_uid,
            state=RunState(run_state.state),
            timestamp=run_state.timestamp,
        )


def to_export_data_v1(
    export_data, since
):  # type: (ExportData, Optional[str]) -> ExportDataV1
    dags = export_data.dags
    dagruns = defaultdict(list)
    for dr in export_data.dag_runs:
        dagruns[dr.dag_id].append(dr)

    task_instances = defaultdict(dict)
    for ti in export_data.task_instances:
        task_instances[(ti.dag_id, ti.execution_date)][ti.task_id] = ti

    result = ExportDataV1()
    for dag in dags:  # type: EDag
        dag_id = dag.dag_id

        result.scheduled_job_infos.append(
            ScheduledJobInfo(
                uid=job_uid(dag_id),
                name=dag_id,
                cmd="",
                start_date=dag.start_date,
                end_date=dag.end_date,
                schedule_interval=dag.schedule_interval,
                catchup=dag.catchup,
                depends_on_past=None,
                retries=None,
                active=None,
                create_user=dag.owner,
                create_time=utcnow(),
                update_user=None,
                update_time=None,
                from_file=False,
                deleted_from_file=False,
                list_order=None,
                job_name=dag_id,
            )
        )

        task_defs = {
            task.task_id: _to_task_def(task) for task in dag.tasks
        }  # type: Dict[str, TaskDefinitionInfo]
        upstream_map = list(_get_upstream_map(dag))

        for dagrun in dagruns[dag_id]:
            run_info = _to_dbnd_run(dag, dagrun)

            all_task_run_infos = {}
            task_run_attempt_updates = []
            execution_date = dagrun.execution_date
            tis = task_instances.get((dag_id, execution_date)) or {}
            for task in dag.tasks:  # type: ETask
                task_id = task.task_id

                all_task_run_infos[task_id] = tr = _to_task_run_info(
                    task=task,
                    run_uid=run_info.run_uid,
                    execution_date=execution_date,
                    task_definition_uid=task_defs[task_id].task_definition_uid,
                    is_root=task.task_id in dag.root_task_ids,
                )

                ti = tis.get(task_id)  # type: ETaskInstance
                task_run_attempt_updates.extend(_update_task_run_info(tr, ti))

                if tr.state in TaskRunState.final_states():
                    if ti.log_body:
                        result.logs.append(
                            SaveTaskRunLog(
                                task_run_attempt_uid=tr.task_run_attempt_uid,
                                log_body=ti.log_body,
                            )
                        )

            if not since or dagrun.start_date > since:
                init_args = get_init_run_args(
                    dag,
                    dagrun,
                    all_task_run_infos,
                    run_info,
                    task_defs,
                    upstream_map,
                    task_run_attempt_updates,
                )
                result.init_args.append(init_args)
            #
            if (
                dagrun.state != State.RUNNING
                and dagrun.end_date
                and (not since or dagrun.end_date > since)
            ):
                result.run_states.append(
                    SetRunStateArgs(
                        run_uid=run_info.run_uid,
                        state=RunState(dagrun.state),
                        timestamp=dagrun.end_date,
                    )
                )

            result.task_run_attempt_updates.extend(task_run_attempt_updates)
            logger.info(
                "Exported DagRun {} {} with name {}".format(
                    dag_id, execution_date, run_info.name
                )
            )
            result.updated_runs.append((dag_id, execution_date, run_info.name))
    return result


def get_init_run_args(
    dag,
    dagrun,
    all_task_run_infos,
    run_info,
    task_defs,
    upstream_map,
    task_run_attempt_updates,
):
    # type: (EDag, EDagRun, Dict[str, TaskRunInfo], RunInfo, Dict[str, TaskDefinitionInfo], List[Tuple[str, str]], List[TaskRunAttemptUpdateArgs], str) -> InitRunArgs
    def tr_uid(tid):
        if tid in all_task_run_infos:
            return all_task_run_infos[tid].task_run_uid

        return None

    rels = [
        (tr_uid(t1_id), tr_uid(t2_id))
        for t1_id, t2_id in upstream_map
        if tr_uid(t1_id) and tr_uid(t2_id)
    ]

    all_times = [dagrun.start_date, dagrun.end_date] + [
        u.timestamp for u in task_run_attempt_updates
    ]
    all_times = [t for t in all_times if t]
    task_run_env = TaskRunEnvInfo(
        uid=get_uuid(),
        cmd_line="",
        databand_version="airflow",
        user_code_version=dag.git_commit,
        user_code_committed=dag.is_committed,
        project_root=dag.dag_folder,
        user_data="",
        user=dag.owner,
        machine=dag.hostname,
        heartbeat=max(all_times)
        if all_times
        else utcnow()
        if dagrun.state == State.RUNNING
        else None,
    )
    task_runs_info = TaskRunsInfo(
        run_uid=run_info.run_uid,
        root_run_uid=run_info.root_run.root_run_uid,
        task_run_env_uid=task_run_env.uid,
        task_runs=list(all_task_run_infos.values()),
        task_definitions=list(task_defs.values()),
        parent_child_map=rels,
        upstreams_map=rels,
        targets=[],
    )
    init_args = InitRunArgs(
        run_uid=run_info.run_uid,
        root_run_uid=run_info.root_run.root_run_uid,
        task_runs_info=task_runs_info,
        task_run_env=task_run_env,
        new_run_info=run_info,
    )
    return init_args


def _update_task_run_info(tr, ti):
    # type: (TaskRunInfo, ETaskInstance) -> Iterable[TaskRunAttemptUpdateArgs]
    if not ti:
        return

    tr.state = TaskRunState(ti.state)
    tr.is_skipped = tr.state == TaskRunState.SKIPPED
    if ti.state in (State.SCHEDULED, State.QUEUED, State.NONE, State.UP_FOR_RESCHEDULE):
        try_number = ti.try_number + 1
    else:
        try_number = ti.try_number

    tr.task_run_attempt_uid = uuid.uuid5(
        tr.run_uid, "{}:{}".format(ti.task_id, try_number)
    )
    if ti.start_date:
        yield TaskRunAttemptUpdateArgs(
            task_run_uid=tr.task_run_uid,
            task_run_attempt_uid=tr.task_run_attempt_uid,
            state=TaskRunState.SCHEDULED,
            timestamp=ti.start_date,
        )
    if ti.end_date:
        yield TaskRunAttemptUpdateArgs(
            task_run_uid=tr.task_run_uid,
            task_run_attempt_uid=tr.task_run_attempt_uid,
            state=tr.state,
            timestamp=ti.end_date,
        )


def _to_task_run_info(task, run_uid, execution_date, task_definition_uid, is_root):
    # type: (ETask, uuid.UUID, datetime, uuid.UUID, bool) -> TaskRunInfo
    task_id = task.task_id
    tr = TaskRunInfo(
        task_run_uid=uuid.uuid5(run_uid, task_id),
        task_run_attempt_uid=uuid.uuid5(run_uid, "{}:{}".format(task_id, -1)),
        task_definition_uid=task_definition_uid,
        run_uid=run_uid,
        execution_date=execution_date,
        task_af_id=task_id,
        task_id=task_id,
        task_signature="{}:{}".format(task_id, execution_date.isoformat()),
        name=task_id,
        env="af_env",
        command_line="",
        functional_call="",
        has_downstreams=bool(task.downstream_task_ids),
        has_upstreams=bool(task.upstream_task_ids),
        is_reused=False,
        is_skipped=False,
        is_dynamic=False,
        is_system=False,
        is_root=is_root,
        output_signature="{}:{}".format(task_id, execution_date.isoformat()),
        state=TaskRunState.SCHEDULED,
        target_date=None,
        version="now",
        log_local="",
        log_remote="",
        task_run_params=[],
    )
    return tr


def _to_task_def(task):  # type: (ETask) -> TaskDefinitionInfo
    return TaskDefinitionInfo(
        task_definition_uid=uuid.uuid5(
            NAMESPACE_DBND_TASK_DEF, "{}.{}".format(task.dag_id, task.task_id)
        ),
        name=task.task_id,
        family=task.task_id,
        type=task.task_type,
        class_version="",
        module_source=task.task_module_code,
        module_source_hash=source_md5(task.task_module_code),
        source=task.task_source_code,
        source_hash=source_md5(task.task_source_code),
        task_param_definitions=[],
    )


def _to_dbnd_run(dag, dagrun):  # type: (EDag, EDagRun) -> RunInfo
    run_uid = job_run_uid(dag.dag_id, dagrun.execution_date)

    run = RunInfo(
        run_uid=run_uid,
        job_name=dag.dag_id,
        user=dag.owner,
        name=get_random_name(),
        description=dag.description,
        state=RunState(dagrun.state),
        start_time=dagrun.start_date,
        end_time=dagrun.end_date,
        dag_id=dag.dag_id,
        execution_date=dagrun.execution_date,
        cmd_name=None,
        target_date=None,
        version=None,
        driver_name="af_driver",
        is_archived=False,
        env_name=None,
        cloud_type="af_cloud",
        trigger="manual",
        root_run=RootRunInfo(root_run_uid=run_uid),
        scheduled_run=ScheduledRunInfo(
            scheduled_job_uid=job_uid(dag.dag_id),
            scheduled_date=dagrun.execution_date,
            scheduled_job_dag_run_id=dagrun.dagrun_id,
        ),
    )
    return run


def job_run_uid(dag_id, execution_date):
    return uuid.uuid5(
        NAMESPACE_DBND_RUN, "{}:{}".format(dag_id, execution_date.isoformat())
    )


def job_uid(dag_id):
    return uuid.uuid5(NAMESPACE_DBND_JOB, dag_id)


def _get_upstream_map(dag):  # type: (EDag) -> Iterable[Tuple[str, str]]
    rels = set()

    if not dag:
        return rels

    tasks = {t.task_id: t for t in dag.tasks}

    def get_upstream(task):  # type: (ETask) -> None
        for upstream_task_id in task.upstream_task_ids:  # type: ETask
            t = tasks.get(upstream_task_id)
            if not t:
                continue
            rel = (task.task_id, t.task_id)
            if rel not in rels:
                rels.add(rel)
                get_upstream(t)

    for root_id in dag.root_task_ids:
        t = tasks.get(root_id)
        if t:
            get_upstream(t)
    return rels
