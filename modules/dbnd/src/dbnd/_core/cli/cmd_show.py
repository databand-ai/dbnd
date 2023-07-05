# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.cli.click_utils import _help
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._vendor import click


@click.command()
@click.argument("search", default="")
@click.pass_context
def show_configs(ctx, search):
    """Show and search configurations"""
    _list_tasks(ctx, search, is_config=True)


COMMON_PARAMS = {"task_version", "task_env", "task_target_date"}


def _list_tasks(ctx, search, is_config):
    from dbnd import Config
    from dbnd._core.context.databand_context import new_dbnd_context
    from dbnd._core.parameter.parameter_definition import _ParameterKind

    formatter = ctx.make_formatter()

    with new_dbnd_context():
        tasks = get_task_registry().list_dbnd_task_classes()

    for task_cls in tasks:
        td = task_cls.task_definition
        full_task_family = td.full_task_family
        task_family = td.task_family

        if not (task_family.startswith(search) or full_task_family.startswith(search)):
            continue

        if issubclass(task_cls, Config) != is_config:
            continue

        dl = []
        for param_name, param_obj in td.task_param_defs.items():
            if param_obj.system or param_obj.kind == _ParameterKind.task_output:
                continue
            if not is_config and param_name in COMMON_PARAMS:
                continue
            param_help = _help(param_obj.description)
            dl.append((param_name, param_help))

        if dl:
            with formatter.section(
                "{task_family} ({full_task_family})".format(
                    full_task_family=full_task_family, task_family=task_family
                )
            ):
                formatter.write_dl(dl)

    click.echo(formatter.getvalue().rstrip("\n"))
