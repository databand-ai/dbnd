#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import shlex
import subprocess
import sys

from functools import partial

import six

from dbnd._core.cli.cmd_execute import execute
from dbnd._core.cli.cmd_heartbeat import send_heartbeat
from dbnd._core.cli.cmd_project import project_init
from dbnd._core.cli.cmd_run import run
from dbnd._core.cli.cmd_scheduler_management import schedule
from dbnd._core.cli.cmd_show import show_configs, show_tasks
from dbnd._core.cli.cmd_tracker import tracker
from dbnd._core.cli.cmd_utils import ipython
from dbnd._core.context.bootstrap import _dbnd_exception_handling, dbnd_bootstrap
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.plugin.dbnd_plugins import pm
from dbnd._core.plugin.dbnd_plugins_mng import register_dbnd_plugins
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._vendor import click
from dbnd._vendor.click_didyoumean import DYMGroup


logger = logging.getLogger(__name__)


@click.group(cls=DYMGroup)
def cli():
    dbnd_bootstrap()

    from dbnd import config

    # if we are running from "dbnd" entrypoint, we probably do not need to load Scheduled DAG
    # this will prevent from every airflow command to access dbnd web api
    if config.getboolean("airflow", "auto_disable_scheduled_dags_load"):
        os.environ["DBND_DISABLE_SCHEDULED_DAGS_LOAD"] = "True"
    pass


# project
cli.add_command(project_init)

# run
cli.add_command(run)
cli.add_command(execute)
cli.add_command(ipython)

# show
cli.add_command(show_configs)
cli.add_command(show_tasks)

# tracker
cli.add_command(tracker)

# scheduler management
cli.add_command(schedule)

# heartbeat sender
cli.add_command(send_heartbeat)


@dbnd_handle_errors(exit_on_error=False)
def dbnd_cmd(command, args):
    """
    Invokes the passed dbnd command with CLI args emulation.

    Parameters:
        command (str): the command to be invoked
        args (Union[list, str]): list with CLI args to be emulated (if str is passed, it will be splitted)
    Returns:
        str: result of command execution
    """
    assert command in cli.commands
    if isinstance(args, six.string_types) or isinstance(args, six.text_type):
        args = shlex.split(args, posix=not windows_compatible_mode)
    current_argv = sys.argv
    logger.info("Running dbnd run: %s", subprocess.list2cmdline(args))
    try:
        sys.argv = [sys.executable, "-m", "databand", "run"] + args
        dbnd_bootstrap()
        return cli.commands[command](args=args, standalone_mode=False)
    finally:
        sys.argv = current_argv


dbnd_run_cmd = partial(dbnd_cmd, "run")
dbnd_schedule_cmd = partial(dbnd_cmd, "schedule")


def main():
    _dbnd_exception_handling()
    configure_basic_logging(None)
    register_dbnd_plugins()

    # adding all plugins cli commands
    for commands in pm.hook.dbnd_get_commands():
        for command in commands:
            cli.add_command(command)

    try:
        return cli(prog_name="dbnd")

    except KeyboardInterrupt:
        sys.exit(1)

    except Exception as ex:
        from dbnd._core.failures import get_databand_error_mesage

        msg, code = get_databand_error_mesage(ex=ex, args=sys.argv[1:])
        logger.error(msg)
        if code is not None:
            sys.exit(code)

    except SystemExit as ex:
        if ex.code == 0:
            # databricks can't handle for a reason exit with an exit code.
            exit()
        else:
            sys.exit(ex.code)


if __name__ == "__main__":
    main()
