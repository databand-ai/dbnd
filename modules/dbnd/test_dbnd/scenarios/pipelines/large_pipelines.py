import logging

from time import sleep

from dbnd import pipeline, task
from dbnd._core.commands import log_metric
from dbnd._core.current import current_task_run


logger = logging.getLogger(__name__)


@task
def operation_int(
    input_a, input_b=0, pause=0.0, log_metrics=True, external_resources=0
):
    # type: (int, int, float, bool, int) -> int
    if log_metrics:
        log_metric("input_a", input_a)
        log_metric("input_b", input_b)

    tr = current_task_run()
    for i in range(external_resources):
        tr.set_external_resource_urls(
            {"url_%s_%d" % (tr.task.task_id, i): "http://localhost"}
        )
    # logger.info("Running  %s -> operation", input)
    if pause:
        logger.info("Pausing for %s seconds", pause)
        sleep(pause)
    return input_a + input_b


@task
def init_acc_int(input=0):
    # type: (int) -> int
    log_metric("input", input)
    return input


@pipeline
def large_pipe_int(width=10, depth=1, pause=0.0):
    # type: (int, int, float) -> int
    res = init_acc_int()
    for i in range(width):
        acc = init_acc_int()
        for d in range(depth):
            acc = operation_int(acc, i, pause)
        res = operation_int(res, acc, pause)

    return res


@pipeline
def shallow_wide_pipe_int(width=25, pause=0):
    # type: (int, int) -> int
    return large_pipe_int(width=width, pause=pause)


@pipeline
def deep_narrow_pipe_int(depth=25, pause=0):
    # type: (int, int) -> int
    return large_pipe_int(width=1, depth=depth, pause=pause)


@pipeline
def deep_wide_pipe_int(width=25, depth=25, pause=0):
    # type: (int, int, int) -> int
    return large_pipe_int(width=width, depth=depth, pause=pause)


@task
def spawn_sub_pipelines(pipe_num=20):
    for i in range(pipe_num):
        logger.info("Running pipeline %s", i)
        large_pipe_int.dbnd_run(
            task_version="%s.%s" % (current_task_run().task.task_version, i)
        )


if __name__ == "__main__":
    print("Inline pipe execution: %s" % large_pipe_int(width=10, depth=1))
