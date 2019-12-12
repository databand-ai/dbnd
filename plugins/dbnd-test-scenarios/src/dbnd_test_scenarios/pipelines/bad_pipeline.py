from dbnd import pipeline, task


@task
def successful_task_1():
    return 1


@task
def successful_task_2():
    return 2


@task
def successful_task_3():
    return 3


@task
def successful_task_4():
    return 4


@task
def bad_task():
    raise Exception("An explicit error for crushing this pipeline.")


@pipeline
def bad_pipe_int():
    successful_task_1()
    successful_task_2()
    successful_task_3()
    successful_task_4()
    return bad_task()
