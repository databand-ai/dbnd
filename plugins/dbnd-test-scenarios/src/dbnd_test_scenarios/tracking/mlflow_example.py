# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import os

from random import randint, random

from mlflow import (
    active_run,
    end_run,
    get_tracking_uri,
    log_artifacts,
    log_metric,
    log_param,
    start_run,
)
from mlflow.tracking import MlflowClient

from dbnd import task


logger = logging.getLogger(__name__)


@task
def task_with_mflow(check_time: datetime.datetime = datetime.datetime.now()) -> str:
    logger.info("Running MLFlow tracking integration check!")
    logger.info("MLFlow tracking URI: {}".format(get_tracking_uri()))

    start_run()

    # params
    log_param("param1", randint(0, 100))
    log_param("param2", randint(0, 100))

    # metrics
    log_metric("foo1", random())
    log_metric("foo1", random() + 1)
    log_metric("foo2", random())
    log_metric("foo2", random() + 1)

    # artifacts
    if not os.path.exists("outputs"):
        os.makedirs("outputs")
    with open("outputs/test1.txt", "w") as f1, open("outputs/test2.txt", "w") as f2:
        f1.write("hello")
        f2.write("world!")
    log_artifacts("outputs")

    # Show metadata & data from the mlflow tracking store:
    service = MlflowClient()
    run_id = active_run().info.run_id
    run = service.get_run(run_id)
    logger.info("Metadata & data for run with UUID %s: %s" % (run_id, run))

    end_run()
    logger.info("MLFlow example completed!")


#
# from dbnd_task
# @task
# def mlflow_example():
#     pass

if __name__ == "__main__":
    task_with_mflow()
