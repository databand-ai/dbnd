from __future__ import print_function

import logging
import os
import subprocess

import six

from dbnd._core.current import get_databand_context
from dbnd._core.errors.friendly_error.task_execution import failed_to_run_cmd
from dbnd._core.log.logging_utils import override_log_formatting, raw_log_formatting


logger = logging.getLogger(__name__)


def _print_log(msg):
    print(msg, end="")


def run_cmd(
    cmd,
    name="process",
    env=None,
    stdout_handler=_print_log,
    return_code=0,
    shell=False,
    **kwargs
):
    if env:
        os_env = os.environ.copy()
        os_env.update(env)
        kwargs["env"] = os_env

    sp = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        universal_newlines=True,
        shell=shell,
        **kwargs
    )

    with raw_log_formatting():
        for l in iter(sp.stdout.readline, ""):
            stdout_handler(l)
    returned_code = sp.wait()

    if isinstance(cmd, six.string_types):
        cmd_str = cmd
    else:
        cmd_str = subprocess.list2cmdline(cmd)

    # Check spark-submit return code. In Kubernetes mode, also check the value
    # of exit code in the log, as it may differ.
    if return_code is not None and returned_code != return_code:
        raise failed_to_run_cmd(name=name, cmd_str=cmd_str, return_code=returned_code)
