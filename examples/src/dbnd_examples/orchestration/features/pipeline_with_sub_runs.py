import logging

from dbnd import dbnd_config, get_databand_context, pipeline, task
from dbnd._core.context.databand_context import DatabandContext


@task
def say_hello(user="sdfsd"):
    greeting = "Hey, {}!".format(user)
    logging.info(greeting)
    return greeting


@task
def calculate_num_of_greetings(input=2):
    return input * 2


@pipeline
def greetings_pipeline(num_of_greetings):
    results = []
    for i in range(num_of_greetings):
        v = say_hello(user="user {}".format(i))
        results.append(v)


@task
def greetings_pipeline_subrun(num_of_greetings):
    assert isinstance(num_of_greetings, int)

    dc = get_databand_context()  # type: DatabandContext
    dc.settings.run.submit_driver = False
    if hasattr(dc.env.remote_engine, "in_cluster"):
        logging.info("Setting remote in cluster False")
        dc.env.remote_engine.in_cluster = False
    if hasattr(dc.env.local_engine, "in_cluster"):
        logging.info("Setting local_engine in cluster False")
        dc.env.local_engine.in_cluster = False
    logging.info("ENGINE: %s", dc.env.local_engine)
    logging.info("ENGINE: %s", dc.env.remote_engine)

    greetings_pipeline.task(num_of_greetings=num_of_greetings).dbnd_run()
    return "OK"


@pipeline
def say_hello_pipe():
    num_of_greetings = calculate_num_of_greetings()
    # we can't use num_of_greetigs value here - we are inside pipeline build phase
    v = greetings_pipeline(
        num_of_greetings=num_of_greetings, task_generator_fields=["num_of_greetings"]
    )

    return v


if __name__ == "__main__":
    with dbnd_config(
        {
            say_hello_pipe.task.task_version: "now",
            calculate_num_of_greetings.task.task_version: "1",
        }
    ):
        say_hello_pipe.dbnd_run()
