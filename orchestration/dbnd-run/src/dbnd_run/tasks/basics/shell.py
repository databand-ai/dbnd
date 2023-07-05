# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import shlex
import signal
import subprocess

from subprocess import list2cmdline
from typing import Any, Dict, List, Optional

import six

from dbnd import log_metric, parameter, task
from dbnd._core.current import try_get_current_task_run
from dbnd._core.errors import DatabandConfigError
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.utils.basics.signal_utils import safe_signal
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd_run.errors.task_execution import failed_to_run_cmd


logger = logging.getLogger(__name__)


@task(popen_kwargs=parameter(empty_default=True)[Dict[str, str]])
def bash_cmd(
    cmd=None,
    args=None,
    check_retcode=0,
    cwd=None,
    env=None,
    dbnd_env=True,
    output_encoding="utf-8",
    popen_kwargs=None,
    wait_for_termination_s=5,
    shell=False,
):
    # type:( str, List[str], Optional[int], str, Dict[str,str], bool, str, Dict[str,Any]) -> int
    if popen_kwargs is None:
        popen_kwargs = dict()
    popen_kwargs = popen_kwargs.copy()

    if cmd and args:
        raise DatabandConfigError("You should not provide cmd and args ")

    if cmd:
        if shell:
            args = cmd
        else:
            args = shlex.split(cmd)
    elif args:
        args = list(map(str, args))
        cmd = list2cmdline(args)
        if shell:
            args = cmd

    logger.info("Running: " + cmd)  # To simplify rerunning failing tests

    if dbnd_env and DatabandRun.has_instance():
        env = env or os.environ.copy()
        dbnd_env_vars = DatabandRun.get_instance().get_context_spawn_env()
        logger.info(
            "Exporting the following env vars:\n%s",
            "\n".join(["{}={}".format(k, v) for k, v in dbnd_env_vars.items()]),
        )
        env.update()

    def preexec_fn():
        if windows_compatible_mode:
            return
        # Restore default signal disposition and invoke setsid
        for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
            if hasattr(signal, sig):
                safe_signal(getattr(signal, sig), signal.SIG_DFL)
        os.setsid()

    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        universal_newlines=True,
        env=env,
        preexec_fn=preexec_fn,
        cwd=cwd,
        shell=shell,  # nosec B602 (Ignore this for bandit as this is code used for orchestration)
        **popen_kwargs
    )

    try:
        task_run = try_get_current_task_run()
        if task_run:
            task_run.task.process = process

        logger.info("Process is running, output:")
        # While command is running let read it's output
        output = []
        while True:
            line = process.stdout.readline()
            if line == "" or line == b"":
                break
            line = safe_decode(line, output_encoding).rstrip()

            logger.info("out: %s", line)
            # keep last 1000 lines only
            output.append(line)
            if len(output) > 1500:
                output = output[-1000:]

        returncode = process.wait()
        logger.info("Command exited with return code %s", process.returncode)
        if check_retcode is not None and returncode != check_retcode:
            raise failed_to_run_cmd(
                "Bash command failed", cmd_str=cmd, return_code=returncode
            )
        return returncode
    except Exception:
        logger.info("Received interrupt. Terminating subprocess and waiting")
        try:
            process.terminate()
            process.wait(wait_for_termination_s)
        except Exception:
            pass
        raise


def safe_decode(line, output_encoding):
    if isinstance(line, six.string_types):
        return line
    return line.decode(output_encoding)


@task(popen_kwargs=parameter(empty_default=True)[Dict[str, str]])
def bash_script(
    script=None,
    check_retcode=0,
    cwd=None,
    env=None,
    dbnd_env=True,
    output_encoding="utf-8",
    popen_kwargs=None,
):
    # type:( str, Optional[int],str, Dict[str,str], bool, str, Dict[str,Any]) -> int

    # we need a working folder to create bash script
    task_run = try_get_current_task_run()
    if task_run:
        script_dir = str(task_run.task_run_executor.local_task_run_root)
    else:
        script_dir = None

    bash_script_path = os.path.join(script_dir, "bash_cmd.sh")
    with open(bash_script_path, "wb") as bs:
        bs.write(bytes(script, "utf_8"))

    log_metric("bash_script", bash_script_path)

    logger.info("Bash script location: %s", bash_script_path)
    args = ["bash", bash_script_path]
    return bash_cmd.func(
        args=args,
        check_retcode=check_retcode,
        cwd=cwd or script_dir,
        env=env,
        dbnd_env=dbnd_env,
        output_encoding=output_encoding,
        popen_kwargs=popen_kwargs,
    )
