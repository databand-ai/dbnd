import logging

from dbnd import pipeline, task


logger = logging.getLogger(__name__)


@task
def operation_x(x_input="x"):
    # type: (str) -> Tuple[str, str]
    logger.info("Running  %s -> operation_x", x_input)
    return x_input, "{} -> operation_x".format(x_input)


@task
def operation_y(y_input="y"):
    # type: (str) -> str
    logger.info("Running %s -> operation_y", y_input)
    return "{} -> operation_y".format(y_input)


@task
def operation_z(z_input=""):
    # type: (str) -> str
    logger.info("Running  %s -> operation_z", z_input)
    return "operation_z({})".format(z_input)


@pipeline
def pipe_operations(pipe_argument="pipe"):
    z = operation_z(pipe_argument)
    _, x = operation_x(z)
    y = operation_y(x)

    # this operation is not wired to any outputs or return values
    # but we need it to run, so it will be "related" to pipe_operations automatically
    operation_z("standalone")

    # you will see y outputs as pipe_argument output in UI
    return y


@pipeline
def pipeline_of_pipelines():
    pipe = pipe_operations(pipe_argument="pipe")
    different_pipe = pipe_operations(pipe_argument="different_pipe")
    return pipe, different_pipe


@pipeline
def pipeline_into_pipeline():
    pipe = pipe_operations(pipe_argument="pipe")
    mega_pipe = pipe_operations(pipe_argument=pipe)  # it's going to be Y of pipe
    return mega_pipe


if __name__ == "__main__":
    print("Inline pipe execution: %s" % pipe_operations("inplace"))
