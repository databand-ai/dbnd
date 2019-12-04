from dbnd import run_task, task


@task
def task_that_spawns(value=1.0):
    # type: (float)-> float

    value_task = task_that_runs_inline.task(value=value)
    run_task(value_task)

    return value_task.result.read_pickle() + 0.1


@task
def task_that_runs_inline(value=1.0):
    # type: (float)-> float
    return value + 0.1
