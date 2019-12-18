import datetime
import logging

from time import sleep

from dbnd import config, databand_lib_path, override, pipeline, task
from dbnd.tasks.basics import dbnd_sanity_check
from dbnd_docker.docker.docker_task import DockerRunTask


logger = logging.getLogger(__name__)


@task(
    task_config=dict(
        kubernetes=dict(
            pod_yaml=override(
                databand_lib_path("conf", "kubernetes-pod-tensorflow.yaml")
            ),
            trap_exit_file_flag=override("/output/training_logs/main-terminated"),
            limits=override({"nvidia.com/gpu": 1}),
        )
    )
)
def task_with_custom_k8s_yml_gpu(
    check_time=datetime.datetime.now(), sleep_time_sec=120
):
    # type: ( datetime.datetime, int)-> str
    config.log_current_config(as_table=True)
    logger.info("Running Kube Sanity Check!")
    if sleep_time_sec:
        logger.info("sleeping for %s", sleep_time_sec)
        sleep(sleep_time_sec)
    return "Databand checked at %s" % check_time


class ExampleDockerNativeTask(DockerRunTask):
    command = "echo hi"
    image = "bash:4.4.23"


@task(
    task_config=dict(
        kubernetes=dict(
            # tolerations=[
            #     dict(
            #         key="special_gpu",
            #         operator="Equal",
            #         value="true",
            #         effect="NoSchedule",
            #     )
            # ],
        )
    )
)
def dbnd_kube_check(check_time=datetime.datetime.now(), sleep_time_sec=120):
    # type: ( datetime.datetime, int)-> str
    config.log_current_config(as_table=True)
    logger.info("Running Kube Sanity Check!")
    if sleep_time_sec:
        logger.info("sleeping for %s", sleep_time_sec)
        sleep(sleep_time_sec)
    return "Databand checked at %s" % check_time


@pipeline
def example_dockerized_pipeline():
    return {"native": ExampleDockerNativeTask(), "simple": dbnd_sanity_check()}


if __name__ == "__main__":
    t = dbnd_kube_check.task()
    t.dbnd_run()
    print(t)
