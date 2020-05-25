from dbnd._core.decorator.func_task_decorator import pipeline, task


@pipeline
def matryoshka():
    r1 = a(1)
    r2 = a(r1)
    a(r2)


@pipeline
def a(param):
    return b(param)


@pipeline
def b(param):
    return c(param)


@task
def c(param):
    return param + 1
