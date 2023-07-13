# © Copyright Databand.ai, an IBM Company 2022

from dbnd import dbnd_tracking
from dbnd._core.current import try_get_current_task_run


def assert_tracked_scenario(func, job_name, run_name=None):
    with dbnd_tracking(job_name=job_name, run_name=run_name):
        actual_task_run = try_get_current_task_run()
        func()
    assert actual_task_run
    return actual_task_run
