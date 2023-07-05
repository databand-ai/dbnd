# © Copyright Databand.ai, an IBM Company 2022
from dbnd_run.cli.cmd_execute import execute
from dbnd_run.cli.cmd_heartbeat import send_heartbeat
from dbnd_run.cli.cmd_project import project_init
from dbnd_run.cli.cmd_run import cmd_run
from dbnd_run.cli.cmd_scheduler_management import schedule
from dbnd_run.cli.cmd_show import show_tasks
from dbnd_run.cli.cmd_utils import collect_logs


def add_dbnd_run_cli(cli):
    # project
    cli.add_command(project_init)

    # run
    cli.add_command(cmd_run)
    cli.add_command(execute)

    # show
    cli.add_command(show_tasks)

    # heartbeat sender
    cli.add_command(send_heartbeat)

    cli.add_command(schedule)

    # error reporting
    cli.add_command(collect_logs)
