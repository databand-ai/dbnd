from dbnd import log_metric, pipeline, task


@task
def get_some_data():
    return 10


@task
def log_some_data(num):
    log_metric("aaaaaa", num)


@task
def calc_and_log(num):
    result = num + 1
    log_metric("bbbbbb", result)


@pipeline
def simple_pipeline():
    x = get_some_data()
    log_some_data(x)
    calc_and_log(x)
