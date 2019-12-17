import uuid

from collections import defaultdict

from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.tracking.airflow_sync import ExportData, SaveTaskRunLog, SetRunStateArgs
from dbnd._core.tracking.tracking_info_convertor import source_md5
from dbnd._core.tracking.tracking_info_objects import (
    TaskDefinitionInfo,
    TaskRunEnvInfo,
    TaskRunInfo,
)
from dbnd._core.tracking.tracking_info_run import RootRunInfo, RunInfo, ScheduledRunInfo
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_uuid
from dbnd._vendor.namesgenerator import get_random_name
from dbnd.api.tracking_api import (
    InitRunArgs,
    ScheduledJobInfo,
    TaskRunAttemptUpdateArgs,
    TaskRunsInfo,
)


NAMESPACE_DBND = uuid.uuid5(uuid.NAMESPACE_DNS, "databand.ai")
NAMESPACE_DBND_JOB = uuid.uuid5(NAMESPACE_DBND, "job")
NAMESPACE_DBND_RUN = uuid.uuid5(NAMESPACE_DBND, "run")
NAMESPACE_DBND_TASK_DEF = uuid.uuid5(NAMESPACE_DBND, "task_definition")


class AFState(object):
    NONE = None
    REMOVED = "removed"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"


def to_export_data(export_data, since):  # type: (EData, Optional[str]) -> ExportData
    if not export_data:
        return
    dags = export_data.dags or []
    dagruns = defaultdict(list)
    for dr in export_data.dag_runs or []:
        dagruns[dr.dag_id].append(dr)

    task_instances = defaultdict(dict)
    for ti in export_data.task_instances or []:
        task_instances[(ti.dag_id, ti.execution_date)][ti.task_id] = ti

    result = ExportData()
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
                dagrun.state != AFState.RUNNING
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
        if dagrun.state == AFState.RUNNING
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
    if ti.state in (
        AFState.SCHEDULED,
        AFState.QUEUED,
        AFState.NONE,
        AFState.UP_FOR_RESCHEDULE,
    ):
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
