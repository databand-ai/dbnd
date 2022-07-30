# © Copyright Databand.ai, an IBM Company 2022

"""
This scenario can be used to test weird heartbeat behavior,
in case heartbeat process is failing,
similar "behaviour" can be achieved via running this task on the driver machine (pod)
```
dbnd run dbnd_test_scenarios.pipelines.errors.heartbeat_error_handling.heartbeat_manual --no-submit-tasks --set run.heartbeat_sender_log_to_file=False
```
"""
import datetime
import subprocess
import sys

from time import sleep

from dbnd import dbnd_bootstrap, task
from dbnd._core.current import get_databand_context, try_get_current_task_run


@task
def heartbeat_manual(log=False, check_time=datetime.datetime.now()):
    cmd = [sys.executable, __file__.replace(".pyc", "py")]

    import logging

    logger = logging.getLogger(__name__)
    if log:
        run = try_get_current_task_run().run
        local_heartbeat_log_file = run.run_local_root.partition(
            name="hearbeat_manual.log"
        )
        heartbeat_log_file = local_heartbeat_log_file
        heartbeat_log_fp = heartbeat_log_file.open("w")
        stdout = heartbeat_log_fp
        logger.error(
            "Starting heartbeat with log at %s using cmd: %s",
            heartbeat_log_file,
            subprocess.list2cmdline(cmd),
        )
    else:
        stdout = None
        logger.error("Starting MANUAL using cmd: %s", subprocess.list2cmdline(cmd))

    sp = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT)

    sleep(10)
    exit_code = sp.wait()
    if exit_code == 0:
        logger.error(" MANUAL finished with 0")
    else:
        logger.error(" MANUAL finished with %s", exit_code)


if __name__ == "__main__":
    # this part will be executed by direct call from the heartbeat_manual task
    # via Popen
    print("Message via print stdout", file=sys.stdout)
    print("Message via print __stderr__", file=sys.__stderr__)
    print("Message via print stderr", file=sys.stderr)
    import logging

    logger = logging.getLogger(__name__)
    logging.info("Message via logging root")

    logger.info("Message via logger")
    from dbnd import config

    with config({"core": {"tracker": ""}}):
        dbnd_bootstrap()

        context = get_databand_context()
        print("CONTEXT CONTEXT CONTEXT")
        print("CONTEXT :Message via print stdout", file=sys.stdout)
        print("CONTEXT :Message via print __stderr__", file=sys.__stderr__)
        print("CONTEXT :Message via print stderr", file=sys.stderr)
        logging.info("CONTEXT : Message via logging root")
        logger.info("CONTEXT : Message via logger")

        tracking_store = get_databand_context().tracking_store
