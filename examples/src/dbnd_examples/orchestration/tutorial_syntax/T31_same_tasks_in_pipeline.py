import logging

from dbnd import config, new_dbnd_context, pipeline, task


logger = logging.getLogger(__name__)


@task
def operation_A(x_input="x"):
    logger.info("Running  %s -> operation_x", x_input)
    if x_input == "ha":
        raise Exception()
    return "{} -> operation_x".format(x_input)


@pipeline
def pipe_A_operations(pipe_argument="pipe"):
    z = operation_A(pipe_argument)
    x = operation_A(z)
    y = operation_A(x, task_name="zzz")

    # this operation is not wired to any outputs or return values
    # but we need it to run, so it will be "related" to pipe_operations automatically
    operation_A("standalone")

    # you will see y outputs as pipe_argument output in UI
    return y


if __name__ == "__main__":
    with config({"zzz": {"x_input": "ha2"}}):
        operations_task = pipe_A_operations.task(task_version="now")
        operations_task.dbnd_run()
