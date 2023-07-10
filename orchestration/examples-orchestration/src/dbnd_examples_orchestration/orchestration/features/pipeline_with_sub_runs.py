# © Copyright Databand.ai, an IBM Company 2022

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
    return results


@task
def greetings_pipeline_subrun(num_of_greetings):
    # this is task, so num_of_greetings is going to be evaluated
    assert isinstance(num_of_greetings, int)

    dc = get_databand_context()  # type: DatabandContext
    # if you are running in "submission" mode, you should cancel that for the new run.
    dc.settings.run.submit_driver = False

    greetings_pipeline.task(num_of_greetings=num_of_greetings).dbnd_run()
    return "OK"


@pipeline
def say_hello_pipe():
    num_of_greetings = calculate_num_of_greetings()
    # we can't use num_of_greetigs value here - we are inside pipeline build phase
    v = greetings_pipeline_subrun(num_of_greetings=num_of_greetings)

    return v


if __name__ == "__main__":
    with dbnd_config(
        {
            say_hello_pipe.task.task_version: "now",
            calculate_num_of_greetings.task.task_version: "1",
        }
    ):
        say_hello_pipe.dbnd_run()
