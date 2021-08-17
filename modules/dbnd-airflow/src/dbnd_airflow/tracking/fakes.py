import attr


@attr.s
class FakeTaskRun(object):
    """
    This is a workaround for using `tracking_store.save_task_run_log`
    cause it require a TaskRun with task_run_attempt_uid attr.
    Should be solved with refactoring the tracking_store interface.
    """

    task_run_attempt_uid = attr.ib()

    task_run_uid = attr.ib(default=None)

    task_af_id = attr.ib(default=None)

    run = attr.ib(default=None)

    task = attr.ib(default=None)

    start_time = attr.ib(default=None)

    finished_time = attr.ib(default=None)


@attr.s
class FakeTask(object):
    task_name = attr.ib()

    task_id = attr.ib()

    task_is_system = False


@attr.s
class FakeRun(object):
    """
    This is a workaround for using `tracking_store.set_task_run_state`
    cause it require a TaskRun with run attr.
    Should be solved with refactoring the tracking_store interface.
    """

    source = attr.ib()
