import datetime
import logging

from time import sleep

from dbnd import config, databand_lib_path, override, task


logger = logging.getLogger(__name__)


@task(
    task_config=dict(
        kubernetes=dict(
            pod_yaml=override(
                databand_lib_path("conf", "kubernetes-pod-tensorflow.yaml")
            ),
            trap_exit_file_flag=override("/output/training_logs/main-terminated"),
            limits=override({"nvidia.com/gpu": 1}),
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


if __name__ == "__main__":
    t = dbnd_kube_check.task()
    t.dbnd_run()
    print(t)
