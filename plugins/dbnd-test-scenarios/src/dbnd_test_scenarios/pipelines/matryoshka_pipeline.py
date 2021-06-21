from dbnd._core.task_build.dbnd_decorator import pipeline, task


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
    return d(param)


@task
def d(param):
    return e(param)


@task
def e(param):
    return f(param)


@task
def f(param):
    return g(param)


@task
def g(param):
    return param + 1
