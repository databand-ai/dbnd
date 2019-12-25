#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys

from dbnd import dbnd_bootstrap
from dbnd._core.cli.cmd_execute import execute
from dbnd._core.cli.cmd_heartbeat import send_heartbeat
from dbnd._core.cli.cmd_project import project_init
from dbnd._core.cli.cmd_run import run
from dbnd._core.cli.cmd_scheduler_management import schedule
from dbnd._core.cli.cmd_show import show_configs, show_tasks
from dbnd._core.cli.cmd_utils import ipython
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.plugin.dbnd_plugins import pm, register_dbnd_plugins
from dbnd._vendor import click
from dbnd._vendor.click_didyoumean import DYMGroup


logger = logging.getLogger(__name__)


# backward compatibility where code is old and docker image is new
class AliasedGroup(DYMGroup):
    def get_command(self, ctx, cmd_name):
        known_aliases = {
            "init_project": "project-init",
            "initdb": "db init",
            "create_user": "db user-create",
            "run_remote": "run",
        }
        cmd_name = known_aliases.get(cmd_name, cmd_name)

        return super(AliasedGroup, self).get_command(ctx, cmd_name)


@click.group(cls=AliasedGroup)
def cli():
    dbnd_bootstrap()
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

# scheduler management
cli.add_command(schedule)

# heartbeat sender
cli.add_command(send_heartbeat)


def main():
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
