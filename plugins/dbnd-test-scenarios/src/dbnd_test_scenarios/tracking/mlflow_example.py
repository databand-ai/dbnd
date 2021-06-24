import logging

from random import randint, random

from mlflow import (
    active_run,
    end_run,
    get_tracking_uri,
    log_metric,
    log_param,
    start_run,
)
from mlflow.tracking import MlflowClient

from dbnd import task


logger = logging.getLogger(__name__)


@task
def mlflow_example():
    logger.info("Running MLFlow example!")
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
    mlflow_example()
