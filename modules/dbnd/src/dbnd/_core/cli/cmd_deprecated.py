# © Copyright Databand.ai, an IBM Company 2022

from dbnd._vendor import click


def created_deprecated_command(cmd):
    @click.command(
        name=cmd,
        context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
    )
    def f():
        print(f"'dbnd {cmd}' not supported any more, please use 'dbnd-web {cmd}'")

    return f


def add_deprecated_commands(cli):
    for cmd in ["db", "webserver", "command"]:
        cli.add_command(created_deprecated_command(cmd))
