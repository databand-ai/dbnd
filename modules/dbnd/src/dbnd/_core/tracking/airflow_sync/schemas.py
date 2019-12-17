import attr

from dbnd._core.utils.timezone import utcnow


@attr.s
class ExportData(object):
    init_args = attr.ib(factory=list)
    task_run_attempt_updates = attr.ib(factory=list)
    scheduled_job_infos = attr.ib(factory=list)
    updated_runs = attr.ib(factory=list)
    logs = attr.ib(factory=list)  # type: List[SaveTaskRunLog]
    run_states = attr.ib(factory=list)  # type: List[SetRunStateArgs]
    timestamp = attr.ib(factory=utcnow)


@attr.s
class SaveTaskRunLog(object):
    task_run_attempt_uid = attr.ib()
    log_body = attr.ib()


@attr.s
class SetRunStateArgs(object):
    run_uid = attr.ib()
    state = attr.ib()
    timestamp = attr.ib()
